using System;
using System.Diagnostics;
using System.IO;

class Program
{
    static void Main()
    {
        string filePath = "20250313_대용량 데이터 전송 테스트.txt";

        if (!File.Exists(filePath))
        {
            Console.WriteLine($"파일 없음: {filePath}");
            return;
        }

        var stopwatch = Stopwatch.StartNew(); //시간 측정

        try
        {
            string content = File.ReadAllText(filePath); // 파일 내용 읽기
            stopwatch.Stop(); //시간 측정 종료

            Console.WriteLine(content);
            Console.WriteLine($"소요 시간: {stopwatch.ElapsedMilliseconds} ms");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"오류: {ex.Message}");
        }
    }
}
