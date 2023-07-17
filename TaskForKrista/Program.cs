using System;
using System.IO.Compression;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Npgsql;

class Program
{
    static async Task Main(string[] args)
    {//подключение к базе
        string connString = "Server=localhost;Port=5432;Database=DataFromJson;User Id=postgres;Password=123456;";
        NpgsqlConnection conn = new NpgsqlConnection(connString);
        try
        {
            Console.WriteLine("введите дату начала:");
            string lastUpdateFrom = Console.ReadLine();
            Console.WriteLine("введите дату окончания:");
            string lastUpdateTo = Console.ReadLine();
            // проверяем, есть ли уже данные для этих дат в базе
            string checkSql = $"SELECT COUNT(*) FROM dates WHERE lastupdatefrom1 = '{lastUpdateFrom}' AND lastupdateto1 = '{lastUpdateTo}'";
            NpgsqlCommand checkCommand = new NpgsqlCommand(checkSql, conn);
            conn.Open();
            int count = Convert.ToInt32(checkCommand.ExecuteScalar());
            conn.Close();
            if (count > 0)
            {
                Console.WriteLine("Данные для этих дат уже загружены в базу.");
                return;
            }
            string requestUri = String.Format("https://bus.gov.ru/public-rest/api/epbs/fap?lastUpdateFrom={0}&lastUpdateTo={1}&page=0&size=100", lastUpdateFrom, lastUpdateTo);
            // Создание HttpClient
            HttpClient httpClient = new HttpClient();

            //создание запроса(пустого)
            HttpRequestMessage request = new HttpRequestMessage
            {//присваеваем запрсу адрес
                RequestUri = new Uri(requestUri),
                // метод гет чтоб что-то взять с этого ресурса
                Method = HttpMethod.Get
            };

            // заголовки запроса
            request.Headers.Add("Accept", "*/*");
            request.Headers.Add("Cookie", "5a492b5f63bbf580c9ad30fe5fe6a9e8=44fe5d31e3406805e61655e32c4adfa6");
            request.Headers.Add("User-Agent", "PostmanRuntime/7.32.3");
            request.Headers.Add("Accept-Encoding", "gzip, deflate, br");

            // для получения ответа от асинхронного запрса к серверу
            HttpResponseMessage response = await httpClient.SendAsync(request);

            // записываются все объекты, которые нужно пихнуть в базу
            string responseBody = await response.Content.ReadAsStringAsync();

            // вытаскиваем информацию и записываем в удобном виде
            dynamic json = JsonConvert.DeserializeObject(responseBody);
            // в переменную TotalPages записываютя данные из поля TotalPages 
            int totalPages = json.totalPages;
            Console.WriteLine(totalPages);

            for (int i = 1; i < totalPages; i++)
            {
                string page = i.ToString();
                string requestUri1 = String.Format("https://bus.gov.ru/public-rest/api/epbs/fap?lastUpdateFrom={0}&lastUpdateTo={1}&page={2}&size=100", lastUpdateFrom, lastUpdateTo, page);


                HttpRequestMessage request1 = new HttpRequestMessage
                {//присваеваем запрсу адрес
                    RequestUri = new Uri(requestUri1),
                    // метод гет чтоб что-то взять с этого ресурса
                    Method = HttpMethod.Get
                };

                // заголовки запроса
                request1.Headers.Add("Accept", "*/*");
                request1.Headers.Add("Cookie", "5a492b5f63bbf580c9ad30fe5fe6a9e8=44fe5d31e3406805e61655e32c4adfa6");
                request1.Headers.Add("User-Agent", "PostmanRuntime/7.32.3");
                request1.Headers.Add("Accept-Encoding", "gzip, deflate, br");

                //  HTTP-запрос
                HttpResponseMessage response1 = httpClient.Send(request1);
                string responseBody1 = await response1.Content.ReadAsStringAsync();
                Console.WriteLine(responseBody1);

                // Парсинг
                dynamic json1 = JsonConvert.DeserializeObject(responseBody1);

                dynamic common = json1.content[0];
                dynamic ar = common.planPaymentIndexes;

                //zip архив
                byte[] bytes = Encoding.UTF8.GetBytes(responseBody1);
                using (MemoryStream stream = new MemoryStream(bytes))
                {
                    using (ZipArchive archive = new ZipArchive(File.Create("responseBody1.zip"), ZipArchiveMode.Create))
                    {
                        ZipArchiveEntry entry = archive.CreateEntry("responseBody1.txt");
                        using (Stream entryStream = entry.Open())
                        {
                            stream.CopyTo(entryStream);
                        }
                    }
                }

                // Запись данных в базу
                foreach (var item in ar)
                {
                    string lastUpdateFrom1 = item.lastUpdateFrom;
                    string lastUpdateTo1 = item.lastUpdateTo;
                    string responseBody2 = JsonConvert.SerializeObject(item);
                    // если данные уже есть в базе, то обновляем их, иначе добавляем новые
                    string sql = count > 0 ?
                        $"UPDATE dates SET responsebody1 = '{responseBody2}' WHERE lastupdatefrom1 = '{lastUpdateFrom1}' AND lastupdateto1 = '{lastUpdateTo1}'" :
                        $"INSERT INTO dates (lastupdatefrom1, lastupdateto1, responsebody1) VALUES ('{lastUpdateFrom}', '{lastUpdateTo}', '{responseBody}')";
                    NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                    conn.Open();
                    command.ExecuteNonQuery();
                    conn.Close();
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
}