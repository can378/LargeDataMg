using System;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;
using System.Diagnostics;

class Program
{
    static async Task Main(string[] args)
    {
        await AthenaQueryExample.RunQuery();
    }
}

class AthenaQueryExample
{
    private static string database = "hugedatabase"; //Athena DB 이름
    private static string outputS3 = "s3://hugehugebucket/athena-results/"; // 쿼리 결과 저장 위치. S3에 해당 폴더, 경로가 있는지 확인할 것것
    private static string query = "SELECT * FROM large_data_table";//조회할 쿼리문

    public static async Task RunQuery()
    {
        var client = new AmazonAthenaClient();
        var stopwatch = new Stopwatch(); //전체 시간측정

        var request = new StartQueryExecutionRequest
        {
            QueryString = query,
            QueryExecutionContext = new QueryExecutionContext { Database = database },
            ResultConfiguration = new ResultConfiguration { OutputLocation = outputS3 }
        };

        var response = await client.StartQueryExecutionAsync(request);
        string queryExecutionId = response.QueryExecutionId;

        var queryStopwatch = Stopwatch.StartNew();//쿼리 실행 시간 확인

        // 쿼리 상태 확인
        GetQueryExecutionResponse result;
        do
        {
            await Task.Delay(1000);
            result = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest
            {
                QueryExecutionId = queryExecutionId
            });
        } while (result.QueryExecution.Status.State == "RUNNING");

        if (result.QueryExecution.Status.State == "SUCCEEDED")
        {
            Console.WriteLine("쿼리 성공!!!!!!!!!");
            Console.WriteLine($"쿼리 실행 시간: {queryStopwatch.ElapsedMilliseconds} ms");
            var outputStopwatch = Stopwatch.StartNew();

            var resultsResponse = await client.GetQueryResultsAsync(new GetQueryResultsRequest
            {
                QueryExecutionId = queryExecutionId
            });

            foreach (var row in resultsResponse.ResultSet.Rows)
            {
                foreach (var data in row.Data)
                {
                    Console.Write($"{data.VarCharValue}\t");
                }
                Console.WriteLine();
            }

            outputStopwatch.Stop();
            Console.WriteLine($"결과 출력 시간: {outputStopwatch.ElapsedMilliseconds} ms");
        }
        else
        {
            Console.WriteLine($"쿼리 실패... = {result.QueryExecution.Status.State} - {result.QueryExecution.Status.StateChangeReason}");
        }

        stopwatch.Stop();
        Console.WriteLine($"전체 실행 시간: {stopwatch.ElapsedMilliseconds} ms");//?????
    }
}
