FROM openjdk:8-jdk
ENV JAVA_OPTS '-Xms1g -Xmx2g'
COPY qcos-tool-1.0.jar /root
# ENTRYPOINT ["java", '${JAVA_OPTS}', "-jar", "/root/qcos-tool-1.0.jar"]
ENTRYPOINT java ${JAVA_OPTS} -jar /root/qcos-tool-1.0.jar