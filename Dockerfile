FROM eclipse-temurin:21-jre-jammy
ARG VERSION
ARG REVISION
LABEL org.opencontainers.image.source="https://github.com/The-Pipeline-Framework/pipelineframework-cli" \
      org.opencontainers.image.description="Verify and deploy immutable TPF Releases" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
RUN groupadd --gid 10001 tpf && useradd --uid 10001 --gid tpf --create-home tpf \
    && mkdir -p /work /home/tpf/.tpf /home/tpf/.m2 /home/tpf/.docker \
    && chown -R tpf:tpf /work /home/tpf \
    && chmod 0755 /home/tpf
COPY tpf-cli/target/*-all.jar /opt/tpf/tpf.jar
ENV HOME=/home/tpf DOCKER_CONFIG=/home/tpf/.docker
WORKDIR /work
USER 10001:10001
ENTRYPOINT ["java", "-Duser.home=/home/tpf", "-jar", "/opt/tpf/tpf.jar"]
