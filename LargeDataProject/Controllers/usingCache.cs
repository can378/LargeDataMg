using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

[ApiController]
[Route("api/cache")]
public class UsingCacheController : ControllerBase
{
    private static List<string>? _cachedData = null; // Nullable

    [HttpGet]
    public IActionResult GetCachedData()
    {
        var stopwatch = Stopwatch.StartNew(); // 시간 측정 시작

        if (_cachedData == null)
        {
            _cachedData = System.IO.File.ReadLines("20250313_대용량 데이터 전송 테스트.txt").ToList();
        }

        stopwatch.Stop(); // 시간 측정 종료
        //30ms 정도 소요 파일을 읽고, 데이터를 메모리에 캐싱하는 데 걸리는 시간
        //서버 응답 + 네트워크 전송 + 브라우저 렌더링까지 걸리는 총 시간 132ms정도 소요
        Console.WriteLine("using cache-"+stopwatch.ElapsedMilliseconds + " ms\n" ); 
        return Ok(new { data = _cachedData, timeTaken = stopwatch.ElapsedMilliseconds + " ms" });
    }
}
