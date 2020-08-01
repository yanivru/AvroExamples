using Avro;
using Avro.File;
using Avro.Generic;
using System;
using System.IO;

namespace AvroExamples
{
    public class GenericRecordExample
    {
        public static void Run()
        {
            Console.WriteLine("Running GenericRecord example");

            using (var stream = File.OpenWrite(@"users.avro"))
            {
                var schemaJson = "{\"type\" : \"record\", " +
                    "\"namespace\" : \"myNameSpace\", " +
                    "\"name\" : \"User\", " +
                    "\"fields\" : " +
                    "[" +
                    "{ \"name\" : \"Name\" , \"type\" : \"string\" }," +
                    "{ \"name\" : \"ID\" , \"type\" : \"int\" }]}";

                var recordSchema = (RecordSchema)Schema.Parse(schemaJson);
                using (var writer = DataFileWriter<GenericRecord>.OpenWriter(new GenericDatumWriter<GenericRecord>(recordSchema), stream))
                {
                    var record = new GenericRecord(recordSchema);
                    record.Add(0, "user1");
                    record.Add(1, 1234);
                    writer.Append(record);
                }
            }

            using (var stream = File.OpenRead(@"users.avro"))
            {
                using (var reader = DataFileReader<GenericRecord>.OpenReader(stream, null, (ws, rs) => new GenericDatumReader<GenericRecord>(ws, rs)))
                {
                    PrintHeader(reader);

                    foreach (var entry in reader.NextEntries)
                    {
                        Print(entry);
                    }
                }
            }
        }

        private static void PrintHeader(IFileReader<GenericRecord> reader)
        {
            var schema = (RecordSchema)reader.GetSchema();

            foreach (var field in schema.Fields)
            {
                Console.Write(field.Name + ", ");
            }
            Console.WriteLine();

        }

        private static void Print(GenericRecord entry)
        {
            for (int i = 0; i < entry.Schema.Count; i++)
            {
                Console.Write(entry.GetValue(i) + ", ");
            }
            Console.WriteLine();
        }
    }
}
