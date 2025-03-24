using System;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;
using System.Diagnostics;
using System.Collections.Generic;

class Program
{
    static async Task Main(string[] args)
    {
        await AthenaQueryExample.RunQuery();
    }
}

class AthenaQueryExample
{
    private static string database = "hugedatabase";
    private static string outputS3 = "s3://hugehugebucket/athena-results/";
    private static string query = "SELECT ROW_NUMBER() OVER() AS rownum, * FROM large_data_table";

    public static async Task RunQuery()
    {
        var client = new AmazonAthenaClient();
        var stopwatch = Stopwatch.StartNew();

        var startResponse = await client.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = query,
            QueryExecutionContext = new QueryExecutionContext { Database = database },
            ResultConfiguration = new ResultConfiguration { OutputLocation = outputS3 }
        });

        string queryExecutionId = startResponse.QueryExecutionId;

        // Wait for query to complete
        GetQueryExecutionResponse result;
        do
        {
            await Task.Delay(1000);
            result = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest
            {
                QueryExecutionId = queryExecutionId
            });
        } while (result.QueryExecution.Status.State == "RUNNING");

        if (result.QueryExecution.Status.State != "SUCCEEDED")
        {
            Console.WriteLine($"쿼리 실패...: {result.QueryExecution.Status.State} - {result.QueryExecution.Status.StateChangeReason}");
            return;
        }

        Console.WriteLine("쿼리 성공!");
        var outputStopwatch = Stopwatch.StartNew();

        var rows = new List<Row>();
        string nextToken = null;

        do
        {
            var getResultRequest = new GetQueryResultsRequest
            {
                QueryExecutionId = queryExecutionId,
                NextToken = nextToken
            };

            var response = await client.GetQueryResultsAsync(getResultRequest);
            nextToken = response.NextToken;

            // 헤더는 첫 호출의 첫 Row이므로, 그 이후부터 출력
            if (rows.Count == 0 && response.ResultSet.Rows.Count > 0)
                response.ResultSet.Rows.RemoveAt(0);  // 헤더 제거

            rows.AddRange(response.ResultSet.Rows);

        } while (!string.IsNullOrEmpty(nextToken));

        Console.WriteLine($"총 데이터 수: {rows.Count}");
        foreach (var row in rows)
        {
            foreach (var col in row.Data)
            {
                Console.Write($"{col.VarCharValue}\t");
            }
            Console.WriteLine();
        }

        outputStopwatch.Stop();
        stopwatch.Stop();

        Console.WriteLine($"결과 출력 시간: {outputStopwatch.ElapsedMilliseconds} ms");
        Console.WriteLine($"전체 실행 시간: {stopwatch.ElapsedMilliseconds} ms");
    }
}
