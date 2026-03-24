<?php

namespace Larva\Flysystem\Tos;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\FilesystemOperationFailed;
use League\Flysystem\InvalidVisibilityProvided;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckDirectoryExistence;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToListContents;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use Throwable;
use Tos\Exception\TosClientException;
use Tos\Exception\TosServerException;
use Tos\Model\CopyObjectInput;
use Tos\Model\DeleteObjectInput;
use Tos\Model\Enum;
use Tos\Model\GetObjectACLInput;
use Tos\Model\GetObjectInput;
use Tos\Model\HeadObjectInput;
use Tos\Model\ListObjectsInput;
use Tos\Model\PutObjectACLInput;
use Tos\Model\PutObjectInput;
use Tos\TosClient;

/**
 * 火山引擎 TOS 适配器
 */
class TOSAdapter implements FilesystemAdapter
{
    /**
     * @var string[]
     */
    public const AVAILABLE_OPTIONS = [
        'Cache-Control', 'Content-Disposition', 'Content-Encoding', 'Content-MD5', 'Content-Length', 'ETag', 'Expires',
    ];

    /**
     * 扩展 MetaData 字段
     * @var string[]
     */
    private const EXTRA_METADATA_FIELDS = [
        'etag',
        'content-md5',
    ];

    /**
     * @var TosClient
     */
    private TosClient $client;

    /**
     * @var PathPrefixer
     */
    private PathPrefixer $prefixer;

    /**
     * @var string
     */
    private string $bucket;

    /**
     * @var VisibilityConverter
     */
    private VisibilityConverter $visibility;

    /**
     * @var MimeTypeDetector
     */
    private MimeTypeDetector $mimeTypeDetector;

    /**
     * @var array
     */
    private array $options;

    /**
     * Adapter constructor.
     *
     * @param  TosClient  $client
     * @param  string  $bucket
     * @param  string  $prefix
     * @param  VisibilityConverter|null  $visibility
     * @param  MimeTypeDetector|null  $mimeTypeDetector
     * @param  array  $options
     */
    public function __construct(
        TosClient $client,
        string $bucket,
        string $prefix = '',
        VisibilityConverter $visibility = null,
        MimeTypeDetector $mimeTypeDetector = null,
        array $options = []
    ) {
        $this->client = $client;
        $this->prefixer = new PathPrefixer($prefix);
        $this->bucket = $bucket;
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
        $this->options = $options;
    }

    /**
     * 判断文件对象是否存在
     *
     * @throws FilesystemException
     * @throws UnableToCheckExistence
     */
    public function fileExists(string $path): bool
    {
        try {
            $response = $this->client->headObject(new HeadObjectInput(
                $this->bucket,
                $this->prefixer->prefixPath($path)
            ));
            return $response->getStatusCode() == 200;
        } catch (TosClientException $ex) {
            throw UnableToCheckExistence::forLocation($path, $ex);
        } catch (TosServerException $ex) {
            return false;
        }
    }

    /**
     * 判断目录是否存在
     *
     * @throws FilesystemException
     * @throws UnableToCheckExistence
     */
    public function directoryExists(string $path): bool
    {
        try {
            $prefix = $this->prefixer->prefixDirectoryPath($path);
            $response = $this->client->ListObjects(new ListObjectsInput($this->bucket, 100, $prefix));
            return is_array($response->getContents());
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToCheckDirectoryExistence::forLocation($path, $ex);
        }
    }

    /**
     * 上传
     *
     * @param  string  $path
     * @param  string|resource  $body
     * @param  Config  $config
     */
    private function upload(string $path, $body, Config $config): void
    {
        $key = $this->prefixer->prefixPath($path);
        try {
            $input = new PutObjectInput($this->bucket, $key, $body);
            $input->setACL($this->determineAcl($config));

            $this->client->putObject($input);
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToWriteFile::atLocation($path, '', $ex);
        }
    }

    /**
     * 写入文件到对象
     *
     * @throws UnableToWriteFile
     * @throws FilesystemException
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * 转换ACL
     * @param  Config  $config
     * @return string
     */
    private function determineAcl(Config $config): string
    {
        $visibility = (string) $config->get(Config::OPTION_VISIBILITY, Visibility::PRIVATE);
        return $this->visibility->visibilityToAcl($visibility);
    }

    /**
     * 将流写入对象
     *
     * @param  resource  $contents
     *
     * @throws UnableToWriteFile
     * @throws FilesystemException
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->upload($path, \stream_get_contents($contents), $config);
    }

    /**
     * 读取对象
     *
     * @throws UnableToReadFile
     * @throws FilesystemException
     */
    public function read(string $path): string
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $response = $this->client->GetObject(new GetObjectInput($this->bucket, $prefixedPath));
            $result = $response->getContent()->getContents();
            $response->getContent()->close();
            return $result;
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToReadFile::fromLocation($path, $ex->getMessage());
        }
    }

    /**
     * 以流的形式读取对象
     *
     * @throws UnableToReadFile
     * @throws FilesystemException
     */
    public function readStream(string $path)
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $response = $this->client->GetObject(new GetObjectInput($this->bucket, $prefixedPath));
            $stream = fopen('php://temp', 'r+');
            fwrite($stream, (string)$response->getContent());
            rewind($stream);
            return $stream;
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToReadFile::fromLocation($path, $ex->getMessage());
        }
    }

    /**
     * 删除对象
     *
     * @throws UnableToDeleteFile
     * @throws FilesystemException
     */
    public function delete(string $path): void
    {
        try {
            $this->client->deleteObject(new DeleteObjectInput($this->bucket, $this->prefixer->prefixPath($path)));
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 删除目录
     *
     * @throws UnableToDeleteDirectory
     */
    public function deleteDirectory(string $path): void
    {
        $this->client->deleteObject(new DeleteObjectInput($this->bucket, $this->prefixer->prefixPath($path)));
    }

    /**
     * 创建目录
     *
     * @throws UnableToCreateDirectory
     */
    public function createDirectory(string $path, Config $config): void
    {
        $this->upload(rtrim($path, '/').'/', '', $config);
    }

    /**
     * 设置对象可见性
     *
     * @throws InvalidVisibilityProvided
     */
    public function setVisibility(string $path, string $visibility): void
    {
        try {
            $input = new PutObjectACLInput(
                $this->bucket,
                $this->prefixer->prefixPath($path),
                $this->visibility->visibilityToAcl($visibility)
            );
            $this->client->putObjectAcl($input);
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToSetVisibility::atLocation($path, $ex->getMessage(), $ex);
        }
    }

    /**
     * 获取对象可见性
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function visibility(string $path): FileAttributes
    {
        $objectOutput = $this->client->getObjectACL(new GetObjectACLInput($this->bucket, $this->prefixer->prefixPath($path)));

        foreach ($objectOutput->getGrants() as $grant) {
            if ($grant->getPermission() === Enum::ACLPublicRead || $grant->getPermission() === Enum::ACLPublicReadWrite) {
                return new FileAttributes($path, null, Visibility::PUBLIC);
            }
        }

        return new FileAttributes($path, null, Visibility::PRIVATE);
    }

    /**
     * 获取文件元数据
     * @param  string  $path
     * @param  string  $type
     * @return void
     */
    private function fetchFileMetadata(string $path, string $type): ?FileAttributes
    {
        try {
            $prefixedPath = $this->prefixer->prefixPath($path);
            $output = $this->client->headObject(new HeadObjectInput($this->bucket, $prefixedPath));
            $mimetype = $output->getContentType() ?? null;
            $fileSize = $output->getContentLength() ?? null;
            $fileSize = $fileSize === null ? null : (int) $fileSize;
            $dateTime = $output->getLastModified() ?? null;
            $lastModified = $dateTime ? strtotime($dateTime) : null;
            return new FileAttributes(
                $path,
                $fileSize,
                null,
                $lastModified,
                $mimetype,
                $this->extractExtraMetadata($output->getMeta())
            );
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToRetrieveMetadata::create($path, $type, '');
        }
    }

    /**
     * 导出扩展 Meta Data
     * @param  array  $metadata
     * @return array
     */
    private function extractExtraMetadata(array $metadata): array
    {
        $extracted = [];
        foreach (static::EXTRA_METADATA_FIELDS as $field) {
            if (isset($metadata[$field]) && $metadata[$field] !== '') {
                $extracted[$field] = $metadata[$field];
            }
        }
        return $extracted;
    }

    /**
     * 获取对象 mime type
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function mimeType(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_MIME_TYPE);
        if ($attributes->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }
        return $attributes;
    }

    /**
     * 获取对象最后修改时间
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function lastModified(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_LAST_MODIFIED);
        if ($attributes->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }
        return $attributes;
    }

    /**
     * 获取对象大小
     *
     * @throws UnableToRetrieveMetadata
     * @throws FilesystemException
     */
    public function fileSize(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_FILE_SIZE);
        if ($attributes->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }
        return $attributes;
    }

    /**
     * 列出对象
     *
     * @param  string  $path
     * @param  bool  $deep
     * @return iterable<StorageAttributes>
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        $response = $this->listObjects($prefixedPath, $deep);
        // 处理目录
        foreach ($response['CommonPrefixes'] ?? [] as $prefix) {
            yield new DirectoryAttributes($prefix['Prefix']);
        }
        //处理文件
        foreach ($response['Contents'] ?? [] as $content) {
            yield new FileAttributes(
                $content['Key'],
                intval($content['Size']),
                null,
                strtotime($content['LastModified'])
            );
        }
    }

    /**
     * 列出对象
     * @param  string  $directory
     * @param  bool  $recursive
     * @return array
     */
    private function listObjects(string $directory = '', bool $recursive = false): array
    {
        $result = [];
        try {
            $objectInput = new ListObjectsInput($this->bucket, 1000, $directory);
            $objectInput->setDelimiter('/');
            $output = $this->client->listObjects($objectInput);
            $result['NextMarker'] = $output->getNextMarker();
            $result['MaxKeys'] = $output->getMaxKeys();
            $result['Prefix'] = $output->getPrefix();
            // 处理目录
            foreach ($output->getCommonPrefixes() ?? [] as $prefix) {
                $result['CommonPrefixes'][] = ['Prefix' => $prefix->getPrefix()];
            }
            //处理文件
            foreach ($output->getContents() ?? [] as $content) {
                $result['Contents'][] = [
                    'Key' => $content->getKey(),
                    'LastModified' => $content->getLastModified(),
                    'ETag' => $content->getETag(),
                    'Size' => $content->getSize(),
                    'StorageClass' => $content->getStorageClass(),
                ];
            }
            unset($output);
        } catch (TosClientException|TosServerException $ex) {
            UnableToListContents::atLocation($directory, $recursive, $ex);
        }
        return $result;
    }

    /**
     * 移动对象到新位置
     *
     * @throws UnableToMoveFile
     * @throws FilesystemException
     */
    public function move(string $source, string $destination, Config $config): void
    {
        try {
            $this->copy($source, $destination, $config);
            $this->delete($source);
        } catch (FilesystemOperationFailed $exception) {
            throw UnableToMoveFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * 复制对象到新位置
     *
     * @throws UnableToCopyFile
     * @throws FilesystemException
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            $destination = $this->prefixer->prefixPath($destination);
            $this->client->copyObject(new CopyObjectInput(
                $this->bucket,
                $destination,
                $this->bucket,
                $this->prefixer->prefixPath($source)
            ));
        } catch (TosClientException|TosServerException $ex) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $ex);
        }
    }

    /**
     * 获取存储桶名称.
     * @return string
     */
    public function getBucket(): string
    {
        return $this->bucket;
    }

    /**
     * 获取客户端.
     * @return TosClient
     */
    public function getClient(): TosClient
    {
        return $this->client;
    }
}
