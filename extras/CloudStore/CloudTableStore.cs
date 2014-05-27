using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Edument.CQRS;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudStore
{
	public class CloudTableStore : IEventStore
	{
		private readonly CloudTable _aggregates;
		private readonly CloudTable _events;

		public CloudTableStore(string connectionStringName = "DevelopmentStorageConnectionString")
		{
			var account = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting(connectionStringName));
			var client = account.CreateCloudTableClient();

			_aggregates = client.GetTableReference("aggregates");
			_aggregates.CreateIfNotExists();

			_events = client.GetTableReference("events");
			_events.CreateIfNotExists();
		}

		public IEnumerable LoadEventsFor<TAggregate>(Guid id)
		{
			var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, id.ToString()));
			return from dte in _events.ExecuteQuery(query)
				   select EventHelper.DeserializeEvent(dte.Properties["Type"].StringValue, dte.Properties["Data"].StringValue);
		}

		public void SaveEventsFor<TAggregate>(Guid? id, int eventsLoaded, ArrayList newEvents)
		{
			if (newEvents.Count == 0)
				return;

			var aggregateId = id ?? EventHelper.GetAggregateIdFromEvent(newEvents[0]);
			foreach (var e in newEvents)
				if (EventHelper.GetAggregateIdFromEvent(e) != aggregateId)
					throw new InvalidOperationException(
						"Cannot save events reporting inconsistent aggregate IDs");

			_aggregates.Execute(TableOperation.InsertOrMerge(
				new DynamicTableEntity(typeof(TAggregate).AssemblyQualifiedName,
				aggregateId.ToString())));

			var batch = new TableBatchOperation();
			for (var i = 0; i < newEvents.Count; i++)
			{
				var e = newEvents[i];
				batch.Insert(
				new DynamicTableEntity(aggregateId.ToString(), (eventsLoaded + i).ToString(CultureInfo.InvariantCulture))
				{
					Properties = new Dictionary<string, EntityProperty>
					{
						{ "Type", new EntityProperty(e.GetType().AssemblyQualifiedName) },
						{ "Data", new EntityProperty(EventHelper.SerializeEvent(e)) }
					}
				});
			}
			_events.ExecuteBatch(batch);
		}
	}
}
