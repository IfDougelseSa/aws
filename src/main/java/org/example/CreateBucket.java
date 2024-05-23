package org.example;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

public class CreateBucket {

    static GetInputs config = new GetInputs();

    public static void main(String[] args) throws Exception {


        String region = config.getLabRegion();
        // Nome do bucket obtido a partir da configuração
        String bucketName = config.getBucketName();

        // Criar o cliente S3 com a região padrão
        S3Client s3 = S3Client.builder()
                .region(Region.of(region))
                .build();

        // Verifica se o bucket já existe usando HeadBucket
        if (!bucketExisting(s3, bucketName)) {
            createBucket(s3, bucketName);  // Cria o bucket
        }

        s3.close(); // Fecha o cliente S3

    }

    public static boolean bucketExisting(S3Client s3, String bucketName) {
        boolean check = true;
        System.out.println("Head Bucket operation... ");

        try {
            HeadBucketRequest request = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            HeadBucketResponse result = s3.headBucket(request);

            if (result.sdkHttpResponse().statusCode() == 200) {
                System.out.println(" Este bucket já existe! ");
            }
        } catch (AwsServiceException awsEx) {
            switch (awsEx.statusCode()) {
                case 404:
                    System.out.println(" Esse bucket não existe.");
                    check = false;
                    break;
                case 400:
                    System.out.println(awsEx.getMessage());
                    System.out.println(" Indica que você está tentando acessar um bucket de uma região diferente da onde o bucket está localizado.");
                    break;
                case 403:
                    System.out.println(" Erros de permissão ao acessar o bucket.");
                    break;
            }
        }
        return check;
    }

    public static void createBucket(S3Client s3Client, String bucketName) {

        System.out.format("\nCriando bucket: %s\n\n", bucketName);

        try {
            S3Waiter s3Waiter = s3Client.waiter();

            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Cria o bucket usando a solicitação
            s3Client.createBucket(bucketRequest);

            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            System.out.format("Aguardando... ");
            // Aguarda até que o bucket seja criado e imprime a resposta
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.format(" Bucket \"%s\" está pronto.\n", bucketName);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
