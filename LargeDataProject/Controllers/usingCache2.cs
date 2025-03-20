using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

[ApiController]
[Route("api/cache2")]
public class UsingCacheController2 : ControllerBase
{
    private readonly IMemoryCache _cache;
    private readonly string _cacheKey = "CachedData"; // 캐시 키

    public UsingCacheController2(IMemoryCache cache)
    {
        _cache = cache;
    }

    [HttpGet]
    public IActionResult GetCachedData()
    {
        var stopwatch = Stopwatch.StartNew(); // 시간 측정 시작

        if (!_cache.TryGetValue(_cacheKey, out List<string>? cachedData)) // 캐시에서 데이터 가져오기
        {
            cachedData = System.IO.File.ReadLines("20250313_대용량 데이터 전송 테스트.txt").ToList();
            
            var cacheEntryOptions = new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10), // 10분 후 캐시 자동 만료
                SlidingExpiration = TimeSpan.FromMinutes(2) // 2분 동안 사용하지 않으면 캐시 삭제
            };

            _cache.Set(_cacheKey, cachedData, cacheEntryOptions); // 캐시에 데이터 저장
        }

        stopwatch.Stop(); // 시간 측정 종료
        Console.WriteLine($"using cache2 - {stopwatch.ElapsedMilliseconds} ms");

        return Ok(new { data = cachedData, timeTaken = stopwatch.ElapsedMilliseconds + " ms" });
    }
}
