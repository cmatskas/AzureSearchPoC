using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using System;
using System.Collections.Generic;

namespace AzureSearchPoC
{
    class Program
    {
        private static string SearchServiceName = "<TheAzureSearchServiceName>";
        private static string SearchServiceAdminApiKey = "<TheAzureSearchAPIKey>";
        private static string AzureSQLConnectionString = @"<TheSQLDbConnectionString>";
        private static string AzureTableConnetionString = @"<TheTableStorageConnectionString>";
        private static string AzureBlobConnectionString = @"<TheBlobStorageConnectionString>";

        static void Main(string[] args)
        {
            SearchServiceClient searchService = new SearchServiceClient(
                searchServiceName: SearchServiceName,
                credentials: new SearchCredentials(SearchServiceAdminApiKey));

            Console.WriteLine("Creating indexes...");
            Console.WriteLine("Creating SQL Index");
            Index sqlIndex = CreateSqlIndex(searchService);

            Console.WriteLine("Creating SQL data source...");
            DataSource sqlDataSource = CreateSqlDataSource();

            // The data source does not need to be deleted if it was already created,
            // but the connection string may need to be updated if it was changed
            searchService.DataSources.CreateOrUpdateAsync(sqlDataSource).Wait();

            Indexer sqlIndexer = CreateSqlIndexer(searchService, sqlIndex, sqlDataSource.Name);
            // We created the indexer with a schedule, but we also
            // want to run it immediately
            Console.WriteLine("Running Azure SQL indexer...");
            searchService.Indexers.RunAsync(sqlIndexer.Name).Wait();

            Console.WriteLine(string.Empty);
            Console.WriteLine("========================================");
            Console.WriteLine("Creating Table Storage Index...");
            Index storageIndex = CreateTableStorageIndex(searchService);

            Console.WriteLine("Creating Table Storage data source...");
            DataSource tableDataSource = CreateStorageTableDataSource();
            searchService.DataSources.CreateOrUpdateAsync(tableDataSource).Wait();
            Indexer tableIndexer = CreateStorageTableIndexer(searchService, storageIndex, tableDataSource.Name);

            Console.WriteLine(string.Empty);
            Console.WriteLine("========================================");
            Console.WriteLine("Creating Combined SQL and Blob Index...");
            Index combinedIndex = CreateCombinedIndex(searchService);
            Console.WriteLine("Creating Blob data source...");
            DataSource blobDataSource = CreateBlobDataSource();
            searchService.DataSources.CreateOrUpdateAsync(blobDataSource).Wait();

            // The data source does not need to be deleted if it was already created,
            // but the connection string may need to be updated if it was changed
            searchService.DataSources.CreateOrUpdateAsync(sqlDataSource).Wait();

            Console.WriteLine("Creating combined sql and blob data indexers...");
            Indexer blobIndexer = CreateStorageBlobIndexer(searchService, combinedIndex, blobDataSource.Name);
            Indexer combinedSqlIndexer = CreateCombinedSqlIndexer(searchService, combinedIndex, sqlDataSource.Name);

            Console.WriteLine("Running Azure blob and sql indexers...");
            searchService.Indexers.RunAsync(blobIndexer.Name).Wait();
            searchService.Indexers.RunAsync(combinedSqlIndexer.Name).Wait();

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();

            Environment.Exit(0);
        }

        private static Index CreateCombinedIndex(SearchServiceClient searchService)
        {
            Index index = new Index(
                name: "sql-blob-index",
                fields: GetSqlFields(),
                suggesters: new List<Suggester> { GetSuggester() }); //only one suggester per index

            // If we have run the sample before, this index will be populated
            // We can clear the index by deleting it if it exists and creating
            // it again
            bool exists = searchService.Indexes.ExistsAsync(index.Name).GetAwaiter().GetResult();
            if (exists)
            {
                searchService.Indexes.DeleteAsync(index.Name).Wait();
            }
            searchService.Indexes.CreateAsync(index).Wait();
            return index;
        }

        private static Index CreateTableStorageIndex(SearchServiceClient searchService)
        {
            Index index = new Index(
                name: "storage-users",
                fields: GetStorageFields(),
                suggesters: new List<Suggester> { GetSuggester() });

            bool exists = searchService.Indexes.ExistsAsync(index.Name).GetAwaiter().GetResult();
            if (exists)
            {
                searchService.Indexes.DeleteAsync(index.Name).Wait();
            }
            searchService.Indexes.CreateAsync(index).Wait();
            return index;
        }

        private static Suggester GetSuggester()
        {
            return new Suggester
            {
                Name = "fieldSuggester",
                SearchMode = SuggesterSearchMode.AnalyzingInfixMatching,
                SourceFields = new List<string> { "FirstName", "LastName" }
            };
        }
        private static Index CreateSqlIndex(SearchServiceClient searchService)
        {
            Index index = new Index(
                name: "sql-customers",
                corsOptions: new CorsOptions() { AllowedOrigins = new List<string> { "192.168.1.1" } },
                fields: GetSqlFields(),
                suggesters: new List<Suggester> { GetSuggester() });

            // If we have run the sample before, this index will be populated
            // We can clear the index by deleting it if it exists and creating
            // it again
            bool exists = searchService.Indexes.ExistsAsync(index.Name).GetAwaiter().GetResult();
            if (exists)
            {
                searchService.Indexes.DeleteAsync(index.Name).Wait();
            }
            searchService.Indexes.CreateAsync(index).Wait();
            return index;
        }

        private static Indexer CreateSqlIndexer(SearchServiceClient searchService, Index index, string dataSourceName)
        {
            Console.WriteLine("Creating Azure SQL indexer...");
            var indexer = new Indexer(
                name: "azure-sql-indexer",
                dataSourceName: dataSourceName,
                targetIndexName: index.Name,
                schedule: new IndexingSchedule(TimeSpan.FromDays(1)));
            // Indexers contain metadata about how much they have already indexed
            // If we already ran the sample, the indexer will remember that it already
            // indexed the sample data and not run again
            // To avoid this, reset the indexer if it exists
            var exists = searchService.Indexers.ExistsAsync(indexer.Name).GetAwaiter().GetResult();


            if (exists)
            {
                searchService.Indexers.ResetAsync(indexer.Name).Wait();
            }
            searchService.Indexers.CreateOrUpdateAsync(indexer).Wait();

            return indexer;
        }

        private static Indexer CreateStorageTableIndexer(SearchServiceClient searchService, Index index, string dataSourceName)
        {
            Console.WriteLine("Creating Azure Table indexer...");
            var indexer = new Indexer(
                name: "azure-table-indexer",
                dataSourceName: dataSourceName,
                targetIndexName: index.Name,
                schedule: new IndexingSchedule(TimeSpan.FromDays(1)));

            var exists = searchService.Indexers.ExistsAsync(indexer.Name).GetAwaiter().GetResult();
            if (exists)
            {
                searchService.Indexers.ResetAsync(indexer.Name).Wait();
            }
            searchService.Indexers.CreateOrUpdateAsync(indexer).Wait();
            return indexer;
        }

        private static Indexer CreateStorageBlobIndexer(SearchServiceClient searchService, Index index, string dataSourceName)
        {
            Console.WriteLine("Creating Azure Blob indexer...");
            var indexer = new Indexer(
                name: "azure-blob-indexer",
                dataSourceName: dataSourceName,
                targetIndexName: index.Name,
                fieldMappings: new List<FieldMapping> { new FieldMapping("uniqueblobkey", "CustomerID") },
                schedule: new IndexingSchedule(TimeSpan.FromDays(1)));

            var exists = searchService.Indexers.ExistsAsync(indexer.Name).GetAwaiter().GetResult();
            if (exists)
            {
                searchService.Indexers.ResetAsync(indexer.Name).Wait();
            }
            searchService.Indexers.CreateOrUpdateAsync(indexer).Wait();
            return indexer;
        }

        private static Indexer CreateCombinedSqlIndexer(SearchServiceClient searchService, Index index, string dataSourceName)
        {
            Console.WriteLine("Creating Azure SQL indexer...");
            var indexer = new Indexer(
                name: "azure-combined-sql-indexer",
                dataSourceName: dataSourceName,
                targetIndexName: index.Name,
                schedule: new IndexingSchedule(TimeSpan.FromDays(1)));

            var exists = searchService.Indexers.ExistsAsync(indexer.Name).GetAwaiter().GetResult();
            if (exists)
            {
                searchService.Indexers.ResetAsync(indexer.Name).Wait();
            }
            searchService.Indexers.CreateOrUpdateAsync(indexer).Wait();

            return indexer;
        }

        private static DataSource CreateStorageTableDataSource()
        {
            DataSource dataSource = DataSource.AzureTableStorage(
                name: "azure-table",
                storageConnectionString: AzureTableConnetionString,
                tableName: "users");
            return dataSource;
        }

        private static DataSource CreateSqlDataSource()
        {
            DataSource dataSource = DataSource.AzureSql(
                name: "azure-sql",
                sqlConnectionString: AzureSQLConnectionString,
                tableOrViewName: "SalesLT.Customer");
            dataSource.DataChangeDetectionPolicy = new SqlIntegratedChangeTrackingPolicy();
            return dataSource;
        }

        private static DataSource CreateBlobDataSource()
        {
            DataSource datasource = DataSource.AzureBlobStorage(
                name: "azure-blob",
                storageConnectionString: AzureBlobConnectionString,
                containerName: "search-data");

            return datasource;
        }

        private static IList<Field> GetStorageFields()
        {
            return new List<Field>
            {
                new Field("Key", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsKey = true},
                new Field("ModifiedDate", DataType.DateTimeOffset)
                    { IsSearchable = false, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("NameStyle",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("Title", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("FirstName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("MiddleName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("LastName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("Suffix",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("CompanyName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("SalesPerson",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("EmailAddress",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("Phone", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
            };
        }

        private static IList<Field> GetSqlFields()
        {
            return new List<Field>
            {
                new Field("CustomerID", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsKey = true },
                new Field("ModifiedDate", DataType.DateTimeOffset)
                    { IsSearchable = false, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("NameStyle",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("Title", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("FirstName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("MiddleName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("LastName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("Suffix",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("CompanyName",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("SalesPerson",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("EmailAddress",DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("Phone", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable= true, IsSortable= true },
                new Field("content", DataType.String, AnalyzerName.EnMicrosoft)
                    { IsSearchable = true, IsFilterable=false, IsRetrievable = true, IsFacetable=true, IsSortable=true}
            };
        }
    }
}
