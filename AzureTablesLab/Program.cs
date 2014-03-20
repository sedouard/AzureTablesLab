using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure;

namespace AzureTablesLab
{
    class Program
    {
        static void Main(string[] args)
        {
            DoSingleInsert();
            DoBatchInsert();
            GetAllInParition();
        }

        //Does single insert of entity
        public static void DoSingleInsert()
        {
            //Parse the connection string for a storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
            CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the table if it doesn't exist.
            CloudTable table = tableClient.GetTableReference("people");
            table.CreateIfNotExists();

            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity("Harp", "Walter");
            customer1.Email = "Walter@contoso.com";
            customer1.PhoneNumber = "425-555-0101";

            TableOperation insertOperation = TableOperation.Insert(customer1);
            table.Execute(insertOperation);
        }

        //Does single insert of entity
        public static void DoBatchInsert()
        {
            //Parse the connection string for a storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
            CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the table if it doesn't exist.
            CloudTable table = tableClient.GetTableReference("people");
            table.CreateIfNotExists();

            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity("Smith", "John");
            customer1.Email = "Walter@contoso.com";
            customer1.PhoneNumber = "425-555-0101";

            // Create another customer entity
            CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
            customer2.Email = "Ben@contoso.com";
            customer2.PhoneNumber = "425-555-0102";

            //Create Batch Operation
            //Note that batch operations must all be in the same partition
            //So in this case the last names of customer 2 and 3 have to be the same
            TableBatchOperation batchOperation = new TableBatchOperation();
            batchOperation.Insert(customer1);
            batchOperation.Insert(customer2);

            table.ExecuteBatch(batchOperation);
        }

        //Does single insert of entity
        public static void GetAllInParition()
        {
            //Parse the connection string for a storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
            CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("people");

            TableQuery<CustomerEntity> query = new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith"));

            Console.WriteLine("Here are all the entries for parition key \"Smith\"");
            foreach(CustomerEntity entity in table.ExecuteQuery(query))
            {
                Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email, entity.PhoneNumber);
            }

        }
    }

    
    //Table entity contains the required columns for any table entity plus some
    //read/write methods for entities
    public class CustomerEntity : TableEntity
    {
        public CustomerEntity(string lastName, string firstName)
        {
            //Note how the partition key is the last name
            this.PartitionKey = lastName;
            this.RowKey = firstName;
        }

        public CustomerEntity() { }

        public string Email { get; set; }

        public string PhoneNumber { get; set; }
    }
}
