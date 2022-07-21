using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka.Producer.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class Producer : ControllerBase
    {
        [HttpPost]
        [ProducesResponseType(typeof(string),201)]
        [ProducesResponseType(400)]
        [ProducesResponseType(500)]
        public IActionResult Post([FromQuery] string msg)
        {
            return Created("", SendMessageByKafka(msg));
        }

        private string SendMessageByKafka(string message)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using(var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var sendResult = producer.ProduceAsync("NewOrder", new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();
                    return $"Mensagem '{sendResult.Value}' de '{sendResult.TopicPartitionOffset}'";
                } 
                catch(ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                }
            }
            return string.Empty;
        }
    }
}
