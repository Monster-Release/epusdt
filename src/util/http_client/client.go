package http_client

import (
	"time"

	"github.com/go-resty/resty/v2"
)

// GetHttpClient 获取请求客户端
func GetHttpClient(proxys ...string) *resty.Client {
	client := resty.New()
	// 如果有代理
	if len(proxys) > 0 {
		proxy := proxys[0]
		client.SetProxy(proxy)
	}
	client.SetRetryCount(3)
	client.SetTimeout(time.Second * 10)
	return client
}
