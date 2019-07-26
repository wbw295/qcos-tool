package net.coding.tool.qcos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.model.UploadPartRequest;
import com.qcloud.cos.model.UploadPartResult;
import com.qcloud.cos.region.Region;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PartUpload {

    private BasicSessionCredentials basicSessionCredentials;

    private String region;

    private String bucketName;

    private String key;

    private String sourceFilePath;

    private int fixThread = 30;

    // 100 M
    private long partSize = 100 * 1024 * 1024;

    private COSClient cosClient;

    public PartUpload(BasicSessionCredentials basicSessionCredentials, String region, String bucketName, String key, String sourceFilePath) {
        this.basicSessionCredentials = basicSessionCredentials;
        this.region = region;
        this.bucketName = bucketName;
        this.key = key;
        this.sourceFilePath = sourceFilePath;
        initCosClient();
    }

    public PartUpload(BasicSessionCredentials basicSessionCredentials, String region, String bucketName, String key, String sourceFilePath, int fixThread, long partSize) {
        this(basicSessionCredentials, region, bucketName, key, sourceFilePath);
        this.fixThread = fixThread;
        this.partSize = partSize;
    }

    public void start() throws InterruptedException, ExecutionException {
        LocalDateTime startTime = LocalDateTime.now();

        // init
        String uploadId = initiateMultipartUploadRequest();

        // start

        File file = new File(sourceFilePath);
        long length = file.length();
        long count = (long) Math.ceil(file.length() / (double) partSize);

        log.info("file length: {}, have splited part count: {}; every part bytes: {}", length, count, partSize);
        ExecutorService executor = Executors.newFixedThreadPool(fixThread);

        List<Future<PartETag>> results = Collections.synchronizedList(new ArrayList<Future<PartETag>>());

        for (long i = 0; i < count; i++) {
            long startPos = i * partSize;
            long partNumber = i + 1;
            Task task = new Task(uploadId, startPos, partSize, partNumber);
            Future<PartETag> result = executor.submit(task);
            results.add(result);
        }

        executor.shutdown();

        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        }

        log.info("all parts have uploaded !!!!!!");
        List<PartETag> partETags = new ArrayList<>();
        for (Future<PartETag> result : results) {
            partETags.add(result.get());
        }

        // complete multi part upload
        completeMultipartUploadRequest(uploadId, partETags);

        // shut down cos client
        shutdown();

        LocalDateTime endTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, endTime);
        long seconds = duration.getSeconds();
        double speed = (double) length / seconds / 1024 / 1024;
        BigDecimal speedBd = new BigDecimal(speed).setScale(2, RoundingMode.HALF_UP);
        log.info("duration: {} secs; average speed: {} MB/S", seconds, speedBd);
    }

    private void initCosClient() {
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        cosClient = new COSClient(basicSessionCredentials, clientConfig);
    }

    private String initiateMultipartUploadRequest() {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, key);
        // 设置存储类型, 默认是标准(Standard), 低频(standard_ia)
        request.setStorageClass(StorageClass.Standard);
        log.info("prepare upload file: {} ...", sourceFilePath);
        InitiateMultipartUploadResult initResult = cosClient.initiateMultipartUpload(request);
        // 获取uploadid
        String uploadId = initResult.getUploadId();
        log.info("init get upload id: {}", uploadId);
        return uploadId;
    }

    private void shutdown() {
        cosClient.shutdown();
    }

    private void completeMultipartUploadRequest(String uploadId, List<PartETag> partETags) {
        // 分片上传结束后，调用complete完成分片上传
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
        CompleteMultipartUploadResult completeResult =
                cosClient.completeMultipartUpload(completeMultipartUploadRequest);
        String etag = completeResult.getETag();
        log.info("complete all part upload; ETag: {} !!!!!!", etag);
    }

    private class Task implements Callable<PartETag> {

        private String uploadId;

        private long startPos;

        private long partSize;

        private long partNumber;

        Task(String uploadId, long startPos, long partSize, long partNumber) {
            this.uploadId = uploadId;
            this.startPos = startPos;
            this.partSize = partSize;
            this.partNumber = partNumber;
        }

        @Override
        public PartETag call() throws IOException {
            byte[] partData;
            long actualPartSize = 0;
            try {
                RandomAccessFile rFile = new RandomAccessFile(PartUpload.this.sourceFilePath, "r");
                partData = new byte[(int) partSize];
                rFile.seek(startPos);// 移动指针到每“段”开头
                actualPartSize = rFile.read(partData);
            } catch (IOException e) {
                log.error("error info startPos {}, partSize {}", startPos, partSize);
                log.error("", e);
                throw e;
            }

            // 生成要上传的数据, 这里初始化一个1M的数据
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(PartUpload.this.bucketName);
            uploadPartRequest.setKey(PartUpload.this.key);
            uploadPartRequest.setUploadId(uploadId);
            // 设置分块的数据来源输入流
            uploadPartRequest.setInputStream(new ByteArrayInputStream(partData));
            // 设置分块的长度
            uploadPartRequest.setPartSize(actualPartSize); // 设置数据长度
            uploadPartRequest.setPartNumber((int) partNumber);
            // 假设要上传的part编号是10

            log.debug("start upload part number: {}; partSize: {}; ......", partNumber, actualPartSize);
            UploadPartResult uploadPartResult = PartUpload.this.cosClient.uploadPart(uploadPartRequest);
            PartETag partETag = uploadPartResult.getPartETag();
            log.debug("end upload part number: {}; partSize: {}; ETag: {} !!!!!!", partETag.getPartNumber(), actualPartSize, partETag.getETag());
            return partETag;
        }
    }
}
