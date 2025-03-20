using System;
using System.Collections.Generic;
using System.Diagnostics;
using MySql.Data.MySqlClient;
using System.Text.Json;

class Program
{
    static void Main()
    {
        string connectionString = "Server=127.0.0.1;Port=3305;Database=test;User=test;Password=test;";

        var stopwatch = Stopwatch.StartNew();
        var result = new List<Dictionary<string, object>>();

        try
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                var cmd = new MySqlCommand("SELECT * FROM LargeData", connection);
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var row = new Dictionary<string, object>();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }

                    result.Add(row);
                }
            }

            stopwatch.Stop();


            string jsonData = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
            Console.WriteLine(jsonData);
            Console.WriteLine($"데이터베이스 조회 시간: {stopwatch.ElapsedMilliseconds} ms");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"오류: {ex.Message}");
        }
    }
}
