using System;
using System.IO;
using System.Text;
using System.Xml.Serialization;

namespace CloudStore
{
	public static class EventHelper
	{
		public static object DeserializeEvent(string typeName, string data)
		{
			using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(data)))
			{
				var ser = new XmlSerializer(Type.GetType(typeName));
				ms.Seek(0, SeekOrigin.Begin);
				return ser.Deserialize(ms);
			}
		}

		public static string SerializeEvent(object obj)
		{
			using (var ms = new MemoryStream())
			{
				var ser = new XmlSerializer(obj.GetType());
				ser.Serialize(ms, obj);
				ms.Seek(0, SeekOrigin.Begin);
				using (var sr = new StreamReader(ms))
					return sr.ReadToEnd();
			}
		}

		public static Guid GetAggregateIdFromEvent(object e)
		{
			var idField = e.GetType().GetField("Id");
			if (idField == null)
				throw new Exception("Event type " + e.GetType().Name + " is missing an Id field");
			return (Guid)idField.GetValue(e);
		}
	}
}