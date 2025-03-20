using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;

[ApiController]
[Route("api/database")]
public class UsingDBController : ControllerBase
{
    private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase, // JSON 필드명 camelCase 적용
        WriteIndented = false // JSON 압축 (공백 제거)
    };

    [HttpGet]
    public IActionResult GetFromDB()
    {
        var stopwatch = Stopwatch.StartNew(); // 시간 측정
        var result = new List<Dictionary<string, object>>(); // JSON 데이터를 저장할 리스트

        using (var connection = new MySqlConnection("Server=127.0.0.1;Port=3305;Database=test;User=test;Password=test;"))
        {
            connection.Open();
            var cmd = new MySqlCommand("SELECT * FROM LargeData", connection);
            var reader = cmd.ExecuteReader();

            while (reader.Read())
            {
                var row = new Dictionary<string, object>(reader.FieldCount);

                for (int i = 0; i < reader.FieldCount; i++) // 모든 컬럼 읽기기
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }

                result.Add(row); // 결과 리스트에 추가
            }
        }

        stopwatch.Stop(); // 시간 측정 종료

        //서버 응답 + 네트워크 전송 + 브라우저 렌더링까지 걸리는 총 시간 3초초정도 소요
        Console.WriteLine($"using db - {stopwatch.ElapsedMilliseconds} ms");

        // JSON 변환 속도 최적화 (System.Text.Json 사용)
        string jsonResponse = JsonSerializer.Serialize(new { data = result, timeTaken = $"{stopwatch.ElapsedMilliseconds} ms" }, _jsonOptions);

        return Content(jsonResponse, "application/json");
    }
}
