namespace Respawn.Tests
{
	using System;
	using NPoco;
	using Oracle.ManagedDataAccess.Client;
	using Shouldly;

	public class OracleTests : IDisposable
	{
		private OracleConnection _connection;
		private Database _database;
		private string _createdUser;

		public class foo
		{
			public int value { get; set; }
		}
		public class bar
		{
			public int value { get; set; }
		}

		public OracleTests()
		{
			_createdUser = DateTime.Now.ToString("yyyyMMddHHmmss");
			using (var connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=xe)));User Id=system;Password=123456;"))
			{
				connection.Open();

				using (var cmd = connection.CreateCommand())
				{
					cmd.CommandText = "create user \"" + _createdUser + "\" IDENTIFIED BY 123456";
					cmd.ExecuteNonQuery();
					// We need some permissions in order to execute all the test queries
					cmd.CommandText = "alter user \"" + _createdUser + "\" IDENTIFIED BY 123456 account unlock";
					cmd.ExecuteNonQuery();
					cmd.CommandText = "grant connect, resource to \"" + _createdUser + "\" IDENTIFIED BY 123456";
					cmd.ExecuteNonQuery();
					cmd.CommandText = "grant create table to \"" + _createdUser + "\" IDENTIFIED BY 123456";
					cmd.ExecuteNonQuery();
				}
			}
			_connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=xe)));User Id=" + _createdUser + ";Password=123456;");
			_connection.Open();

			_database = new Database(_connection, DatabaseType.OracleManaged);
		}

		public void ShouldDeleteData()
		{
			_database.Execute("create table \"foo\" (value int)");

			for (int i = 0; i < 100; i++)
			{
				_database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
			}

			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);

			var checkpoint = new Checkpoint
			{
				DbAdapter = DbAdapter.Oracle,
				SchemasToInclude = new[] { _createdUser }
			};
			checkpoint.Reset(_connection);

			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(0);
		}

		public void ShouldIgnoreTables()
		{
			_database.Execute("create table \"foo\" (value int)");
			_database.Execute("create table \"bar\" (value int)");

			for (int i = 0; i < 100; i++)
			{
				_database.Execute("INSERT INTO \"foo\" VALUES (@0)", i);
				_database.Execute("INSERT INTO \"bar\" VALUES (@0)", i);
			}

			var checkpoint = new Checkpoint
			{
				DbAdapter = DbAdapter.Oracle,
				SchemasToInclude = new[] { _createdUser },
				TablesToIgnore = new[] { "foo" }
			};
			checkpoint.Reset(_connection);

			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"foo\"").ShouldBe(100);
			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM \"bar\"").ShouldBe(0);
		}

		public void ShouldExcludeSchemas()
		{
			_database.Execute("create schema a");
			_database.Execute("create schema b");
			_database.Execute("create table a.\"foo\" (value int)");
			_database.Execute("create table b.\"bar\" (value int)");

			for (int i = 0; i < 100; i++)
			{
				_database.Execute("INSERT INTO a.\"foo\" VALUES (" + i + ")");
				_database.Execute("INSERT INTO b.\"bar\" VALUES (" + i + ")");
			}

			var checkpoint = new Checkpoint
			{
				DbAdapter = DbAdapter.Postgres,
				SchemasToExclude = new[] { "a", "pg_catalog" }
			};
			checkpoint.Reset(_connection);

			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.\"foo\"").ShouldBe(100);
			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.\"bar\"").ShouldBe(0);
		}

		public void ShouldIncludeSchemas()
		{
			_database.Execute("create schema a");
			_database.Execute("create schema b");
			_database.Execute("create table a.\"foo\" (value int)");
			_database.Execute("create table b.\"bar\" (value int)");

			for (int i = 0; i < 100; i++)
			{
				_database.Execute("INSERT INTO a.\"foo\" VALUES (" + i + ")");
				_database.Execute("INSERT INTO b.\"bar\" VALUES (" + i + ")");
			}

			var checkpoint = new Checkpoint
			{
				DbAdapter = DbAdapter.Postgres,
				SchemasToInclude = new[] { "b" }
			};
			checkpoint.Reset(_connection);

			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM a.\"foo\"").ShouldBe(100);
			_database.ExecuteScalar<int>("SELECT COUNT(1) FROM b.\"bar\"").ShouldBe(0);
		}

		public void Dispose()
		{
			// Clean up our mess before leaving
			DropUser();

			_connection.Close();
			_connection.Dispose();
			_connection = null;

			_database.Dispose();
			_database = null;
		}

		private void DropUser()
		{
			using (var connection = new OracleConnection("Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=xe)));User Id=system;Password=123456;"))
			{
				connection.Open();

				using (var cmd = connection.CreateCommand())
				{
					// First we need to disconnect the user
					cmd.CommandText = @"SELECT s.sid, s.serial#, s.status, p.spid FROM v$session s, v$process p WHERE s.username = '" + _createdUser + "' AND p.addr(+) = s.paddr";

					var dataReader = cmd.ExecuteReader();
					if (dataReader.Read())
					{
						var sid = dataReader.GetOracleDecimal(0);
						var serial = dataReader.GetOracleDecimal(1);

						cmd.CommandText = "ALTER SYSTEM KILL SESSION '" + sid + ", " + serial + "'";
						cmd.ExecuteNonQuery();

						cmd.CommandText = "drop user \"" + _createdUser + "\" CASCADE";
						cmd.ExecuteNonQuery();
					}
				}
			}
		}
	}
}
