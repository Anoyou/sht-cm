// Service Worker for SHT资源爬取管理平台
const CACHE_NAME = 'sht-manager-v1.0.9';
const STATIC_CACHE_NAME = 'sht-static-v1.0.9';

// 需要缓存的静态资源
const STATIC_ASSETS = [
  '/',
  '/static/css/main.css',
  '/static/js/main.js',
  '/static/manifest.json',
  '/crawler',
  '/categories',
  '/config',
  '/services',
  '/logs'
];

// 需要缓存的API路径（用于离线功能）
const API_CACHE_PATTERNS = [
  '/api/stats/system',
  '/api/config/get',
  '/api/forum/info/batch'
];

// 安装事件 - 缓存静态资源
self.addEventListener('install', event => {
  console.log('Service Worker: Installing...');

  event.waitUntil(
    Promise.all([
      // 缓存静态资源
      caches.open(STATIC_CACHE_NAME).then(cache => {
        console.log('Service Worker: Caching static assets');
        return cache.addAll(STATIC_ASSETS);
      }),
      // 跳过等待，立即激活
      self.skipWaiting()
    ])
  );
});

// 激活事件 - 清理旧缓存
self.addEventListener('activate', event => {
  console.log('Service Worker: Activating...');

  event.waitUntil(
    Promise.all([
      // 清理旧缓存
      caches.keys().then(cacheNames => {
        return Promise.all(
          cacheNames.map(cacheName => {
            if (cacheName !== CACHE_NAME && cacheName !== STATIC_CACHE_NAME) {
              console.log('Service Worker: Deleting old cache:', cacheName);
              return caches.delete(cacheName);
            }
          })
        );
      }),
      // 立即控制所有客户端
      self.clients.claim()
    ])
  );
});

// 拦截网络请求
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // 只处理同源请求
  if (url.origin !== location.origin) {
    return;
  }

  // 处理不同类型的请求
  if (request.method === 'GET') {
    if (isStaticAsset(request.url)) {
      // 静态资源：缓存优先策略
      event.respondWith(cacheFirst(request, STATIC_CACHE_NAME));
    } else if (isAPIRequest(request.url)) {
      // API请求：网络优先策略
      event.respondWith(networkFirst(request, CACHE_NAME));
    } else if (isPageRequest(request)) {
      // 页面请求：网络优先，离线时返回缓存
      event.respondWith(networkFirst(request, STATIC_CACHE_NAME));
    }
  }
});

// 判断是否为静态资源
function isStaticAsset(url) {
  return url.includes('/static/') ||
    url.endsWith('.css') ||
    url.endsWith('.js') ||
    url.endsWith('.png') ||
    url.endsWith('.jpg') ||
    url.endsWith('.svg') ||
    url.endsWith('.ico');
}

// 判断是否为API请求
function isAPIRequest(url) {
  // 排除测试端点，让它们直接通过网络请求
  if (url.includes('/api/logs/test')) {
    return false;
  }
  return url.includes('/api/') ||
    API_CACHE_PATTERNS.some(pattern => url.includes(pattern));
}

// 判断是否为页面请求
function isPageRequest(request) {
  return request.headers.get('accept')?.includes('text/html');
}

// 缓存优先策略
async function cacheFirst(request, cacheName) {
  try {
    const cache = await caches.open(cacheName);
    const cachedResponse = await cache.match(request);

    if (cachedResponse) {
      // 后台更新缓存
      fetch(request).then(response => {
        if (response.ok) {
          cache.put(request, response.clone());
        }
      }).catch(() => {
        // 网络错误时忽略
      });

      return cachedResponse;
    }

    // 缓存中没有，从网络获取
    const response = await fetch(request);
    if (response.ok) {
      cache.put(request, response.clone());
    }
    return response;

  } catch (error) {
    console.error('Cache first strategy failed:', error);
    return new Response('离线模式下资源不可用', {
      status: 503,
      statusText: 'Service Unavailable'
    });
  }
}

// 网络优先策略
async function networkFirst(request, cacheName) {
  try {
    const response = await fetch(request);

    if (response.ok) {
      // 缓存成功的响应
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }

    return response;

  } catch (error) {
    console.log('Network failed, trying cache:', error);

    // 网络失败，尝试从缓存获取
    const cache = await caches.open(cacheName);
    const cachedResponse = await cache.match(request);

    if (cachedResponse) {
      return cachedResponse;
    }

    // 如果是页面请求且缓存中没有，返回离线页面
    if (isPageRequest(request)) {
      return new Response(`
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>离线模式 - SHT资源管理</title>
          <style>
            body { 
              font-family: -apple-system, BlinkMacSystemFont, sans-serif;
              display: flex; 
              justify-content: center; 
              align-items: center; 
              height: 100vh; 
              margin: 0; 
              background: #f8fafc;
              color: #374151;
            }
            .offline-container {
              text-align: center;
              padding: 2rem;
              max-width: 400px;
            }
            .offline-icon {
              font-size: 4rem;
              margin-bottom: 1rem;
            }
            .offline-title {
              font-size: 1.5rem;
              font-weight: 600;
              margin-bottom: 1rem;
            }
            .offline-message {
              margin-bottom: 2rem;
              line-height: 1.6;
            }
            .retry-button {
              background: #3b82f6;
              color: white;
              border: none;
              padding: 0.75rem 1.5rem;
              border-radius: 0.5rem;
              cursor: pointer;
              font-size: 1rem;
            }
            .retry-button:hover {
              background: #2563eb;
            }
          </style>
        </head>
        <body>
          <div class="offline-container">
            <div class="offline-icon">📱</div>
            <h1 class="offline-title">离线模式</h1>
            <p class="offline-message">
              当前网络不可用，您正在使用离线版本。<br>
              部分功能可能受限，请检查网络连接。
            </p>
            <button class="retry-button" onclick="window.location.reload()">
              重新连接
            </button>
          </div>
        </body>
        </html>
      `, {
        headers: { 'Content-Type': 'text/html' }
      });
    }

    return new Response('网络不可用', {
      status: 503,
      statusText: 'Service Unavailable'
    });
  }
}

// 监听消息事件（用于手动缓存更新等）
self.addEventListener('message', event => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }

  if (event.data && event.data.type === 'CACHE_UPDATE') {
    // 手动更新缓存
    event.waitUntil(updateCache());
  }
});

// 更新缓存
async function updateCache() {
  try {
    const cache = await caches.open(STATIC_CACHE_NAME);
    await cache.addAll(STATIC_ASSETS);
    console.log('Cache updated successfully');
  } catch (error) {
    console.error('Cache update failed:', error);
  }
}

// 后台同步（如果支持）
if ('sync' in self.registration) {
  self.addEventListener('sync', event => {
    if (event.tag === 'background-sync') {
      event.waitUntil(doBackgroundSync());
    }
  });
}

async function doBackgroundSync() {
  // 在这里可以执行后台同步任务
  // 比如同步离线时的操作、更新数据等
  console.log('Background sync triggered');
}