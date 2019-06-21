using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace funcBlobToSQL
{
    public static class Function
    {

        [FunctionName("Function")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            string filepath = req.Query["file"];

            var containerName = "stocklist";

            string storageConnection = "DefaultEndpointsProtocol=https;AccountName=stocklistfiles;AccountKey=5wG4EpY8Bke5sChSJ0HHzvXjA/2FA27aCKnLfJx20uxfG7J+CBdOAQ4U4TpTmqlXQv3k+M60bv3K5Js4GKLf5g==;BlobEndpoint=https://stocklistfiles.blob.core.windows.net/";
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(filepath);

             var stream = await blockBlob.OpenReadAsync();

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            SqlConnection conn = null;
            builder.DataSource = "sqlserverdemonstration.database.windows.net";
            builder.UserID = "Neosalpha";
            builder.Password = "neos@123";
            builder.InitialCatalog = "WMS";

         
           

            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{filepath} \n Size: {""} Bytes");

            //if (!filepath.EndsWith(".csv"))
            //{
            //    log.LogInformation($"Blob '{filepath}' doesn't have the .csv extension. Skipping processing.");
            //    return error;
            //}

            var records = new List<Product>();
            using (var Memorystream = new MemoryStream())
            {
                using (var tr = new StreamReader(stream))
                {
                    using (var csv = new CsvReader(tr))
                    {
                        if (csv.Read())
                        {
                            log.LogInformation("Reading CSV");
                            csv.ReadHeader();

                            log.LogInformation($"Blob '{filepath}' found. Uploading to SQL Server");

                            while (csv.Read())
                            {
                                try
                                {

                                    conn = new SqlConnection(builder.ConnectionString);

                                    var record = new Product
                                    {
                                        ProductID = Convert.ToInt32(csv.GetField("ProductID")),
                                        ProductName = csv.GetField("ProductName"),
                                        CostPrice = Convert.ToInt32(csv.GetField("CostPrice")),
                                        SellingPrice = Convert.ToInt32(csv.GetField("SellingPrice")),
                                        Quantity=Convert.ToInt32(csv.GetField("Quantity"))

                                    };


                                    using (SqlConnection con = new SqlConnection(builder.ConnectionString))
                                    {
                                        using (SqlCommand cmd = new SqlCommand("sp_InsertCSVStockDetails", con))
                                        {
                                            cmd.CommandType = CommandType.StoredProcedure;
                                            cmd.Parameters.AddWithValue("@ProductID", record.ProductID);
                                            cmd.Parameters.AddWithValue("@ProductName", record.ProductName);
                                            cmd.Parameters.AddWithValue("@CostPrice", record.CostPrice);
                                            cmd.Parameters.AddWithValue("@SellingPrice", record.SellingPrice);
                                            cmd.Parameters.AddWithValue("@Quantity", record.Quantity);
                                            con.Open();
                                            int a = cmd.ExecuteNonQuery();
                                        }
                                    }

                                  
                                    log.LogInformation($"Blob '{filepath}' uploaded");
                                }
                                catch (SqlException se)
                                {
                                    log.LogInformation($"Exception Trapped: {se.Message}");

                                }
                             
                            }
                        }
                    }
                }
            }


            return filepath != null
                ? (ActionResult)new OkResult()
                : new BadRequestObjectResult("Please pass a filepath on the query string or in the request body");



        }
    }

}
