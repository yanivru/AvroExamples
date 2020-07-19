using Avro;
using Avro.File;
using Avro.Generic;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AvroExamples
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var reader = DataFileReader<GenericRecord>.OpenReader(@"weather.avro"))
            {
                WriteHeader(reader);

                foreach(var entry in reader.NextEntries)
                {
                    Print(entry);
                }
            }
        }

        private static void WriteHeader(IFileReader<GenericRecord> reader)
        {
            var schema = (RecordSchema)reader.GetSchema();

            foreach(var field in schema.Fields)
            {
                Console.Write(field.Name + ", ");
            }
            Console.WriteLine();

        }

        private static void Print(GenericRecord entry)
        {
            for(int i = 0; i < entry.Schema.Count; i++)
            {
                Console.Write(entry.GetValue(i) + ", ");
            }
            Console.WriteLine();
        }
    }
}
