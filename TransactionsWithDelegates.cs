using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;


namespace Library
{
  #region Delegates

	/// <summary>
	/// A delegate for data updates which require a transaction i.e. need to be atomic
	/// </summary>
	public delegate void TransactionedOperationDelegate(TransactionWrapper sender, TransactionArgs args);

	#endregion

	#region Operation Parameters Class
	/// <summary>
	/// Base class for all classes implementing functionality of parameters for transactioned operations
	/// Derive sub-classes from this, and customise with fields etc.
	/// In the transactioned operation, check the type of the object.
	/// </summary>
	public abstract class TransactionedOperation
	{
		public TransactionedOperationDelegate Execute; // cannot use 'event' designation as we are calling this multicast from outside the defining class
	}
	#endregion

	#region Transaction Args
	
	/// <summary>
	/// Lightweight class representing arguments for a transactionable operation
	/// </summary>
	public class TransactionArgs : EventArgs
	{
		public TransactionArgs(IDbConnection connection, IDbTransaction transaction, TransactionedOperation operation)
		{
			this.Connection = connection;
			this.Transaction = transaction;
			this.Operation = operation;
		}

		public readonly IDbConnection Connection;
		public readonly IDbTransaction Transaction;
		public readonly TransactionedOperation Operation;
	}	

	#endregion

	#region TransactionWrapper Class
	/// <summary>
	/// Provides a fairly generic way to wrap any operation within a transaction.
	/// The functionality is implemented using delegates.
	/// </summary>
	public class TransactionWrapper
	{
		#region .ctor
		public TransactionWrapper(IDbConnection connection)
		{
			if (null == connection)
			{
				throw new ArgumentNullException("connection");
			}

			this.Connection = connection;
		}
		#endregion

		#region Fields
		internal readonly IDbConnection Connection;
		#endregion

		/// <summary>
		/// Makes an operation atomic by allowing all data updates it contains easily to share the same transaction
		/// </summary>
		/// <param name="operations">An operation which should use a single transaction</param>
		public void MakeAtomic(TransactionedOperation operation)
		{
			#region Parameter checks
			if (null == operation)
			{
				throw new ArgumentNullException("operation");
			}
			#endregion

			MakeAtomic(new TransactionedOperation[] {operation});
		}

		/// <summary>
		/// Makes one or more potentially disparate operations atomic by allowing them easily to share the same transaction
		/// </summary>
		/// <param name="operations">An array of operations which should share the same transaction</param>
		public void MakeAtomic(TransactionedOperation[] operations)
		{
			#region Parameter checks
			if (null == operations)
			{
				throw new ArgumentNullException("operations");
			}

			for (int i=0; i < operations.Length; ++i)
			{
				TransactionedOperation operation = operations[i];

				if (null == operation)
				{
					throw new ArgumentNullException("operation");
				}

				if (null == operation.Execute)
				{
					throw new ArgumentNullException(String.Format("operation.Execute at index {0}", i));
				}
			}

			#endregion

			#region Main functionality

			this.Connection.Open();
			try
			{
				using (IDbTransaction transaction = this.Connection.BeginTransaction())
				{
					try
					{
						/// Prepare, then call the operation
						/// Any exceptions will cause the transaction to be aborted
						/// 
						/// All relevent objects have been checked for nullness prior to this
						/// 
						foreach (TransactionedOperation operation in operations)
						{					
							TransactionArgs args = new TransactionArgs(Connection, transaction, operation);
							operation.Execute(this, args);

							// TODO: Log the operation here if required.
						}

						transaction.Commit();
					}
					catch (Exception exception)
					{
						transaction.Rollback();

						// TODO: Log the error here

						throw;
					}
				}
			}
			finally
			{
				// Strictly speaking, there is no need for this close here because IDbConnection 
				// implements IDisposable - the connection will be closed when the object is Garbage-Collected
				this.Connection.Close();
			}

			#endregion
		}

	}
	#endregion

}

namespace Application
{
	/// <summary>
	/// Example logging interface
	/// </summary>
	public interface ILog
	{
		void Write(DateTime dateTime, IDbConnection connection, Library.TransactionArgs args);
	}

	/// <summary>
	/// Some kind of custom data set
	/// </summary>
	public class CustomDataSet : DataSet
	{
		public string ExampleData;
	}

	/// <summary>
	/// 
	/// </summary>
	public class CustomTransactionedOperation : Library.TransactionedOperation
	{
		public CustomTransactionedOperation (CustomDataSet dataSet)
		{
			this.Example = dataSet.ExampleData;
		}

		public readonly string Example;
	}

	/// <summary>
	/// A base class for data update operations. Provides logging capabilities.
	/// </summary>
	public abstract class BaseDataUpdate
	{
		public BaseDataUpdate (ILog log)
		{
			this.log = log;
		}

		private ILog log;

		private bool loggingEnabled;
		public bool LoggingEnabled
		{
			get { return loggingEnabled; }
			set { loggingEnabled = value; }
		}

		public virtual void UpdateData(Library.TransactionWrapper sender, Library.TransactionArgs args)
		{
			log.Write(DateTime.Now, sender.Connection, args);
		}
	}

	/// <summary>
	/// An example class which performs data updates.
	/// </summary>
	public class CustomDataUpdate : BaseDataUpdate
	{
		public CustomDataUpdate (ILog log) : base (log) { }

		public override void UpdateData(Library.TransactionWrapper sender, Library.TransactionArgs args)
		{
			CustomTransactionedOperation operation = args.Operation as CustomTransactionedOperation;

			SqlCommand sqlCommand = new SqlCommand();
			sqlCommand.Transaction = args.Transaction as System.Data.SqlClient.SqlTransaction;
			sqlCommand.Connection = args.Connection as System.Data.SqlClient.SqlConnection;

			sqlCommand.CommandText = operation.Example; /* Some suitable text here */

			sqlCommand.ExecuteNonQuery(); // TODO: note the number of rows affected and publish via an event 

			base.UpdateData(sender, args);
		}
	}

	/// <summary>
	/// Example class demonstrating	a standalone method used in data updates.
	/// </summary>
	public class StandaloneDataUpdate
	{
		/// <summary>
		/// This method does not need custom TransactionArgs parameter. 
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="args"></param>
		public void SomeMethod(Library.TransactionWrapper sender, Library.TransactionArgs args)
		{
			SqlCommand sqlCommand = new SqlCommand();
			sqlCommand.Transaction = args.Transaction as System.Data.SqlClient.SqlTransaction;
			sqlCommand.Connection = args.Connection as System.Data.SqlClient.SqlConnection;

			sqlCommand.CommandText = ""; /* Some suitable text here */

			sqlCommand.ExecuteNonQuery();			
		}
	}


	/// <summary>
	/// Some other kid of data set
	/// </summary>
	public class SpecialDataSet : DataSet
	{
		public int MoreExampleData;
	}

	/// <summary>
	/// Example class where the data update method is actaully part of the class. 
	/// This avoid the need to redefine separate classes for the update method and the parameters
	/// but might reduce flexibility.
	/// </summary>
	public class AggregateTransactionedOperation : Library.TransactionedOperation
	{
		public AggregateTransactionedOperation (SpecialDataSet dataSet)
		{
			this.Example = dataSet.MoreExampleData;
		}

		public readonly int Example;

		public static void SelfContainedUpdateData(Library.TransactionWrapper sender, Library.TransactionArgs args)
		{	
			// TODO: Paramter checks

			AggregateTransactionedOperation operation = args.Operation as AggregateTransactionedOperation;

			SqlCommand sqlCommand = new SqlCommand();
			sqlCommand.Transaction = args.Transaction as System.Data.SqlClient.SqlTransaction;
			sqlCommand.Connection = args.Connection as System.Data.SqlClient.SqlConnection;

			sqlCommand.CommandText = ""; /* Some suitable text here */

			sqlCommand.ExecuteNonQuery();
		}
	}

	/// <summary>
	/// Manager class which co-ordinates access to data update operations
	/// </summary>
	public class DataUpdateManager
	{
		private ILog log = null; // Initialise this somewhere...

		/// <summary>
		/// 
		/// </summary>
		/// <param name="dataSet"></param>
		public void UpdateCustomDetails(CustomDataSet dataSet)
		{
			IDbConnection connection = new System.Data.SqlClient.SqlConnection( /* Connection string goes here*/ );
			Library.TransactionWrapper transactionWrapper = new Library.TransactionWrapper(connection);
			
			// Hook the data onto the operation parameters
			CustomDataUpdate customDataUpdate = new CustomDataUpdate(this.log);
			CustomTransactionedOperation operation = new CustomTransactionedOperation(dataSet);
			operation.Execute += new Library.TransactionedOperationDelegate(customDataUpdate.UpdateData);

			transactionWrapper.MakeAtomic(operation);
		}

		/// <summary>
		/// Performs multiple updates in a single transaction, with disparate (but presumably related) data.
		/// </summary>
		/// <param name="dataSet1">first data set</param>
		/// <param name="dataSet2">second data set</param>
		public void UpdateMultipleDetails(CustomDataSet dataSet1, SpecialDataSet dataSet2)
		{
			// Prepare the connection
			IDbConnection connection = new System.Data.SqlClient.SqlConnection( /* Connection string goes here*/ );
			Library.TransactionWrapper transactionWrapper = new Library.TransactionWrapper(connection);
			
			// Hook data for first operation
			CustomDataUpdate customDataUpdate = new CustomDataUpdate(this.log);			
			CustomTransactionedOperation operation1 = new CustomTransactionedOperation(dataSet1);
			operation1.Execute += new Library.TransactionedOperationDelegate(customDataUpdate.UpdateData);

			// Hook data for second operation
			AggregateTransactionedOperation operation2 = new AggregateTransactionedOperation(dataSet2);
			operation2.Execute += new Library.TransactionedOperationDelegate(AggregateTransactionedOperation.SelfContainedUpdateData);

			// Collect together the operations and make them atomic
			Library.TransactionedOperation[] operations = new Library.TransactionedOperation[] {operation1, operation2};
			transactionWrapper.MakeAtomic(operations);
		}
	}
}
