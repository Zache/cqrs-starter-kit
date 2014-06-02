using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using CloudStore;
using Edument.CQRS;
using CafeReadModels;
using Cafe.Tab;

namespace WebFrontend
{
	public static class Domain
	{
		public static MessageDispatcher Dispatcher;
		public static IOpenTabQueries OpenTabQueries;
		public static IChefTodoListQueries ChefTodoListQueries;

		public static void Setup()
		{
			var eventStorage = new CloudTableStore();
			Dispatcher = new MessageDispatcher(eventStorage);

			Dispatcher.ScanInstance(new TabCommandHandlers());

			OpenTabQueries = new OpenTabs();
			Dispatcher.ScanInstance(OpenTabQueries);

			ChefTodoListQueries = new ChefTodoList();
			Dispatcher.ScanInstance(ChefTodoListQueries);

			var aggregates = eventStorage.GetAllAggregates();
			foreach (var agg in aggregates)
				Dispatcher.GetType().GetMethod("ReplayEvents")
					.MakeGenericMethod(agg.Item1)
					.Invoke(Dispatcher, new object[] { agg.Item2 });
		}
	}
}