package net.coding.tool.qcos;

import com.google.gson.Gson;

import com.qcloud.cos.auth.BasicSessionCredentials;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class PartUploadApp {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Map<String, String> envMap = System.getenv();
        log.info("envs: {}", new Gson().toJson(envMap));
        String secretId = envMap.get("tmpSecretId");
        String secretKey = envMap.get("tmpSecretKey");
        String sessionToken = envMap.get("sessionToken");
        String region = envMap.get("region");
        String bucketName = envMap.get("bucketName");
        String key = envMap.get("key");
        String sourceFilePath = envMap.get("sourceFilePath");
        int fixThread = Integer.valueOf(envMap.get("fixThread"));
        long partSize = Long.valueOf(envMap.get("partSize"));
        BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(secretId, secretKey, sessionToken);
        PartUpload partUpload = new PartUpload(basicSessionCredentials, region, bucketName, key, sourceFilePath, fixThread, partSize);
        partUpload.start();
    }
}
