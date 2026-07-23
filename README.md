# flysystem-tos

<p align="center">
    <a href="https://packagist.org/packages/larva/flysystem-tos"><img src="https://poser.pugx.org/larva/flysystem-tos/v/stable" alt="Stable Version"></a>
    <a href="https://packagist.org/packages/larva/flysystem-tos"><img src="https://poser.pugx.org/larva/flysystem-tos/downloads" alt="Total Downloads"></a>
    <a href="https://packagist.org/packages/larva/flysystem-tos"><img src="https://poser.pugx.org/larva/flysystem-tos/license" alt="License"></a>
</p>

这是火山引擎 TOS（Tinder Object Storage）对象存储的 [Flysystem](https://flysystem.thephpleague.com/) 适配器，支持 Flysystem v2/v3。

## 环境要求

- PHP >= 8.0
- Composer 2.0+
- Flysystem v2 或 v3
- 火山引擎 TOS PHP SDK v2.1+

## 安装

```bash
composer require larva/flysystem-tos -vv
```

## 基础用法

### 1. 创建 TOS 客户端

```php
use Tos\TosClient;

$client = new TosClient([
    'region' => 'cn-beijing',
    'endpoint' => 'tos-cn-beijing.volces.com',
    'ak' => 'your-access-key',
    'sk' => 'your-secret-key',
]);
```

### 2. 创建适配器

```php
use Larva\Flysystem\Tos\TOSAdapter;
use Larva\Flysystem\Tos\PortableVisibilityConverter;

$adapter = new TOSAdapter(
    client: $client,
    bucket: 'your-bucket-name',
    prefix: '',                              // 可选，存储路径前缀
    visibility: new PortableVisibilityConverter(), // 可选，可见性转换器
    mimeTypeDetector: null,                 // 可选，MIME 类型检测器
    options: []                             // 可选，额外选项
);
```

### 3. 配合 Filesystem 使用

```php
use League\Flysystem\Filesystem;

$filesystem = new Filesystem($adapter);

// 写入文件
$filesystem->write('path/to/file.txt', 'file contents');

// 读取文件
$contents = $filesystem->read('path/to/file.txt');

// 检查文件是否存在
$exists = $filesystem->fileExists('path/to/file.txt');

// 删除文件
$filesystem->delete('path/to/file.txt');

// 列出目录内容
foreach ($filesystem->listContents('path/to/dir') as $item) {
    echo $item->path() . PHP_EOL;
}
```

## 可见性控制

适配器通过 `VisibilityConverter` 接口将 Flysystem 的可见性（`public` / `private`）映射为 TOS 的 ACL：

| Flysystem 可见性 | TOS ACL |
|------------------|---------|
| `Visibility::PUBLIC` | `public-read` |
| `Visibility::PRIVATE` | `private` |

默认使用 `PortableVisibilityConverter`，你也可以实现 `VisibilityConverter` 接口自定义映射逻辑：

```php
use Larva\Flysystem\Tos\VisibilityConverter;
use League\Flysystem\Visibility;

class CustomVisibilityConverter implements VisibilityConverter
{
    public function visibilityToAcl(string $visibility): string
    {
        return $visibility === Visibility::PUBLIC ? 'public-read' : 'private';
    }

    public function aclToVisibility(string $acl): string
    {
        return $acl === 'public-read' ? Visibility::PUBLIC : Visibility::PRIVATE;
    }

    public function defaultForDirectories(): string
    {
        return Visibility::PUBLIC;
    }
}
```

## 支持的方法

| 方法 | 说明 |
|------|------|
| `write($path, $contents, $config)` | 写入文件 |
| `writeStream($path, $stream, $config)` | 以流的方式写入文件 |
| `read($path)` | 读取文件内容 |
| `readStream($path)` | 以流的方式读取文件 |
| `fileExists($path)` | 判断文件是否存在 |
| `directoryExists($path)` | 判断目录是否存在 |
| `delete($path)` | 删除文件 |
| `deleteDirectory($path)` | 删除目录 |
| `createDirectory($path, $config)` | 创建目录 |
| `setVisibility($path, $visibility)` | 设置文件可见性 |
| `visibility($path)` | 获取文件可见性 |
| `mimeType($path)` | 获取文件 MIME 类型 |
| `lastModified($path)` | 获取文件最后修改时间 |
| `fileSize($path)` | 获取文件大小 |
| `listContents($path, $deep)` | 列出目录内容 |
| `move($source, $destination, $config)` | 移动文件 |
| `copy($source, $destination, $config)` | 复制文件 |

## Laravel 集成

在 Laravel 项目中，可以通过自定义 Filesystem 驱动的方式集成：

```php
// AppServiceProvider::boot()
use Illuminate\Support\Facades\Storage;
use Larva\Flysystem\Tos\TOSAdapter;
use League\Flysystem\Filesystem;

Storage::extend('tos', function ($app, $config) {
    $client = new \Tos\TosClient([
        'region' => $config['region'],
        'endpoint' => $config['endpoint'],
        'ak' => $config['ak'],
        'sk' => $config['sk'],
    ]);

    $adapter = new TOSAdapter($client, $config['bucket'], $config['prefix'] ?? '');

    return new Filesystem($adapter);
});
```

在 `config/filesystems.php` 中添加磁盘配置：

```php
'tos' => [
    'driver' => 'tos',
    'region' => env('TOS_REGION', 'cn-beijing'),
    'endpoint' => env('TOS_ENDPOINT', 'tos-cn-beijing.volces.com'),
    'ak' => env('TOS_AK'),
    'sk' => env('TOS_SK'),
    'bucket' => env('TOS_BUCKET'),
    'prefix' => env('TOS_PREFIX', ''),
],
```

然后在 `.env` 中配置相应的环境变量：

```env
TOS_REGION=cn-beijing
TOS_ENDPOINT=tos-cn-beijing.volces.com
TOS_AK=your-access-key
TOS_SK=your-secret-key
TOS_BUCKET=your-bucket-name
TOS_PREFIX=
```

使用方式：

```php
Storage::disk('tos')->put('file.txt', 'contents');
$contents = Storage::disk('tos')->get('file.txt');
```

## 获取客户端

如需直接操作 TOS SDK，可以获取底层客户端：

```php
$client = $adapter->getClient();
$bucket = $adapter->getBucket();
```

## 贡献

欢迎提交 Issue 和 Pull Request。

## License

[MIT](LICENSE)
