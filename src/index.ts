import * as grpc from '@grpc/grpc-js';
import { Client } from 'minio';
import { join } from 'path';
import fs from 'fs';
import { nanoid } from 'nanoid';
import path from 'path';
import { StorageObject } from '@stvsh/media_pb/storage_object_pb';
import { CreateThumbnailRequest, CreateThumbnailResponse } from '@stvsh/media_pb/thumbnails_pb';
import { ThumbnailsService } from '@stvsh/media_pb/thumbnails_grpc_pb';
import ffmpeg from 'fluent-ffmpeg';
import { minioConfig } from '@stvsh/commons/config/minio';
import { grpcConfig } from '@stvsh/commons/config/grpc';

const minioClient = new Client({
  endPoint: minioConfig.endPoint,
  port: minioConfig.port,
  accessKey: minioConfig.accessKey,
  secretKey: minioConfig.secretKey,
  useSSL: false
});

const thumbnailsBucket = 'videos-thumbnails';
const thumnbnailExtension = '.png';
const tmpDir = join(__dirname, '..', 'tmp');

const downloadVideo = async (bucket: string, object: string): Promise<string> => {
  const fileName = nanoid();
  const outputFile = join(tmpDir, fileName);
  await minioClient.fGetObject(bucket, object, outputFile);

  return outputFile;
};

const uploadThumbnail = async (
  bucket: string,
  object: string,
  inputFile: string
): Promise<void> => {
  await minioClient.fPutObject(bucket, object, inputFile, {});
  return;
};

const cleanFiles = async (files: string[]): Promise<void> => {
  await Promise.all(files.map((file) => fs.promises.unlink(file)));
  return;
};

const createThumbnail = async (inputFile: string): Promise<string> => {
  const fileName = nanoid() + thumnbnailExtension;
  const outputFile = join(tmpDir, fileName);

  return new Promise<string>((resolve, reject) => {
    const proc = ffmpeg(inputFile);
    proc.videoFilters(['thumbnail', 'scale=640:360']);
    proc.frames(1);
    proc.output(outputFile);
    proc.run();

    proc.on('error', (error) => {
      console.error(error.message);
      reject(error);
    });

    proc.on('end', () => {
      resolve(outputFile);
    });
  });
};

const handler = (): grpc.UntypedServiceImplementation => {
  return {
    createThumbnail: async (
      call: grpc.ServerUnaryCall<CreateThumbnailRequest, CreateThumbnailResponse>,
      callback: grpc.sendUnaryData<CreateThumbnailResponse>
    ) => {
      const { sourceVideoObject } = call.request.toObject();

      try {
        if (sourceVideoObject == null) {
          throw new Error('sourceVideoObject missing');
        }

        const { bucket, object } = sourceVideoObject;
        const videoPath = await downloadVideo(bucket, object);
        const thumbnailPath = await createThumbnail(videoPath);
        const thumbnailName = path.parse(thumbnailPath).base;

        await uploadThumbnail(thumbnailsBucket, thumbnailName, thumbnailPath);

        const response = new CreateThumbnailResponse();
        const storageObject = new StorageObject();
        storageObject.setBucket(thumbnailsBucket);
        storageObject.setObject(thumbnailName);
        response.setThumbnailObject(storageObject);

        cleanFiles([videoPath, thumbnailPath]);

        callback(null, response);
      } catch (error) {
        callback(error);
      }
    }
  };
};

const main = async () => {
  const server = new grpc.Server();
  server.addService(ThumbnailsService, handler());
  server.bindAsync(`0.0.0.0:${grpcConfig.port}`, grpc.ServerCredentials.createInsecure(), () => {
    server.start();
  });
};

main();
