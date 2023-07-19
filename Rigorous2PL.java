import java.awt.desktop.AboutEvent;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.jar.Attributes.Name;

public class Rigorous2PL {

	HashMap<Integer,Transaction> transactionMap;
	HashMap<Character,Item> itemMap;
	List<String> listOfOperations; 
	String state[];
	
	public Rigorous2PL(String path) {
		transactionMap=  new HashMap<Integer, Transaction>();
		itemMap =  new HashMap<>();
		listOfOperations = new ArrayList<>();
		state = new String[] {"Active", "Waiting", "Aborted"};
		
		readInstructionsInToObject(path);
		System.out.println("\n\n********** Transactions as in given file ***********");
		displayOperations();
		System.out.println("\n\n********** begin of 2Phase locking protocol ***********\n\n");
		beginProtocol();
	}
	
	/*
	 * Displaying the list of instructions before starting the simulation of protocol
	 */
	public void displayOperations() {
		for(String line: listOfOperations) {
			System.out.println(line);
		}
	}
	

	/* I am reading the external file and storing that in a local ArrayList of string.
	 * Which I am later using to simulate the 2 phase locking protocol.
	 * exception handling has been added to ensure proper termination in case
	 * file is not found.
	 * 
	 */
	
	public void readInstructionsInToObject(String path)  {
		BufferedReader bufferedReader = null;
		try {
			bufferedReader =new BufferedReader(new FileReader(path));
		
		String line;
		
			line = bufferedReader.readLine();
		
			while(line!= null) {
				listOfOperations.add(line);
				line = bufferedReader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(bufferedReader != null)
					bufferedReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	
/*The below is main member method for that implements the 2 phase locking protocol.
 * It reads list of operation from array list object and processes each of them line
 * by line. Switch case is used to perform operation based on type of first character 
 * 
 * r -> read
 * w -> write
 * e -> end (perform commit if possible)
 * b -> begin a new transaction
 * 
 * the read and write cases are primary importance because this is where I am checking if a operation
 * can be allowed to request a lock on item, whether it should wait or die based on timestamp of conflicting 
 * operations.	
 */
	
	public void beginProtocol() {
		for(int i = 0;i<listOfOperations.size();i++) {
			
			char operationType=listOfOperations.get(i).charAt(0);
			int tranId = listOfOperations.get(i).charAt(1) % 48;
			
			switch(operationType) {
				
				case 'b': 
						
						Transaction newTransaction = new Transaction(tranId);
						transactionMap.put(tranId, newTransaction);
						System.out.println(listOfOperations.get(i)+" T"+newTransaction.id+" begins Id="+newTransaction.id+
								". TimeStamp: "+newTransaction.timestamp.toString()+". state: "+state[newTransaction.state]);
						
						break;
					
				case 'r': 
//					int id = listOfOperations.get(i).charAt(1)% 48;// update to read the number
					
					Transaction transactionRead = transactionMap.get(tranId);
					char itemName = listOfOperations.get(i).charAt(3);
					
					if(!itemMap.containsKey(itemName)) {
						Item newItem = new Item(itemName);
						itemMap.put(itemName, newItem);
//						System.out.println(" *******************************new item created and added to HashMap object:" +newItem.itemName);
					}
					
					Item item = itemMap.get(itemName);
					
					if(transactionRead.state == 0) {      // active
							if(checkCanAcquireLock(transactionRead, item, operationType ) == 0) {   // can acquire lock
								transactionRead.locked_item_by_transaction.add(listOfOperations.get(i));
								item.transactions_gained_lock.add(listOfOperations.get(i));
								item.type_of_lock = 1;
								System.out.println(listOfOperations.get(i)+" "+item.itemName+" is read locked by T"+transactionRead.id+".");
							}
							else if (checkCanAcquireLock(transactionRead, item, operationType) == 1) {  // allowed to wait
								transactionRead.state = 1;
								transactionRead.operations_waiting_in_queue.add(listOfOperations.get(i));
								item.transactions_waiting.add(listOfOperations.get(i));
								System.out.println(listOfOperations.get(i)+" T"+transactionRead.id+". is blocked/waiting due to wait-die.");
							}
							else if (checkCanAcquireLock(transactionRead, item, operationType) == 2){
								transactionRead.state = 2;
								removeAnyLocksAcquiredByAbortedTransaction(transactionRead);
								System.out.println(listOfOperations.get(i)+" T"+transactionRead.id+" is aborted due to wait die.");
							}
							
					}
					else if(transactionRead.state == 1) { // in wait queue
//							if (checkCanAcquireLock(transactionRead, item, operationType) == 1) {  // allowed to wait
								transactionRead.operations_waiting_in_queue.add(listOfOperations.get(i));
								item.transactions_waiting.add(listOfOperations.get(i));
								System.out.println(listOfOperations.get(i)+" T"+transactionRead.id+". is blocked/waiting due to wait-die.");
//							}
//							else if (checkCanAcquireLock(transactionRead, item, operationType) == 2){
//								transactionRead.state = 2;
//								removeAnyLocksAcquiredByAbortedTransaction(transactionRead);
//							}
						}
					else if(transactionRead.state == 2) { // aborted
						System.out.println(listOfOperations.get(i)+" T"+transactionRead.id+" already aborted");
						}

						break;
						
						
				case 'w':
					
					Transaction transactionWrite = transactionMap.get(tranId);
					char itemNameInWrite = listOfOperations.get(i).charAt(3);
					
					if(!itemMap.containsKey(itemNameInWrite)) {
						Item newItem = new Item(itemNameInWrite);
						itemMap.put(itemNameInWrite, newItem);
//						System.out.println(" new item created :" +newItem.itemName);
					}
					
					
					Item itemWrite = itemMap.get(itemNameInWrite);
					
					if(transactionWrite.state == 0) {      // active
							if(checkCanAcquireLock(transactionWrite, itemWrite, operationType ) == 0) {   // can acquire lock
								
//								System.out.println("lock granted on :"+itemWrite.itemName+ "  to transaction :"+transactionWrite.id+"    state: "+transactionWrite.state);
								if(itemWrite.transactions_gained_lock.size()>0) {
									System.out.println(listOfOperations.get(i)+" read lock on "+itemWrite.itemName+" by T"+transactionWrite.id+" is upgraded to write lock.");
									itemWrite.transactions_gained_lock = new LinkedList<String>();
								}else {
									System.out.println(listOfOperations.get(i)+" "+itemWrite.itemName+" is write locked by T"+transactionWrite.id+".");
								}
								
								Queue<String> tempQueue =new LinkedList<>();
								for(String ele : transactionWrite.locked_item_by_transaction) {
									if(ele.charAt(3) != itemWrite.itemName)
										tempQueue.add(ele);
								}
								
								transactionWrite.locked_item_by_transaction = new LinkedList<String>(tempQueue);
								
								transactionWrite.locked_item_by_transaction.add(listOfOperations.get(i));
								itemWrite.transactions_gained_lock.add(listOfOperations.get(i));
								itemWrite.type_of_lock = 2;
							}
							else if (checkCanAcquireLock(transactionWrite, itemWrite, operationType) == 1) {  // allowed to wait
								transactionWrite.state = 1;
								transactionWrite.operations_waiting_in_queue.add(listOfOperations.get(i));
								itemWrite.transactions_waiting.add(listOfOperations.get(i));
//								System.out.println("wait for lock on :"+itemWrite.itemName+ "  to transaction :"+transactionWrite.id+"    state: "+transactionWrite.state);
								System.out.println(listOfOperations.get(i)+" T"+transactionWrite.id+". is blocked/waiting due to wait-die.");
							}
							else if (checkCanAcquireLock(transactionWrite, itemWrite, operationType) == 2){
								transactionWrite.state = 2;
//								System.out.println("transaction aborted: "+listOfOperations.get(i));
								removeAnyLocksAcquiredByAbortedTransaction(transactionWrite);
								System.out.println(listOfOperations.get(i)+" T"+transactionWrite.id+" is aborted due to wait die.");
								
//								System.out.println(itemWrite.transactions_gained_lock.size()+"     size");
//								for(String ele: itemWrite.transactions_gained_lock ) {
//									System.out.print(ele+"   reason for failure\n    ");
//								}
								
							}
							
					}
					else if(transactionWrite.state == 1) { // in wait queue
//							if (checkCanAcquireLock(transactionWrite, itemWrite, operationType) == 1) {  // allowed to wait
								transactionWrite.operations_waiting_in_queue.add(listOfOperations.get(i));
								itemWrite.transactions_waiting.add(listOfOperations.get(i));
								System.out.println(listOfOperations.get(i)+" T"+transactionWrite.id+". is blocked/waiting due to wait-die.");
//							}
//							else if (checkCanAcquireLock(transactionWrite, itemWrite, operationType) == 2){
//								transactionWrite.state = 2;
//								removeAnyLocksAcquiredByAbortedTransaction(transactionWrite);
//							}
						}
					else if(transactionWrite.state == 2) { // aborted
						System.out.println(listOfOperations.get(i)+" T"+transactionWrite.id+" is already aborted");
						
						}
					
					
						break;
					
				case 'e':
					Transaction transactionEnd= transactionMap.get(tranId);
					if(transactionEnd.state == 2) {
						System.out.println(listOfOperations.get(i)+" T"+transactionEnd.id+" is already aborted");
					}
					else if(transactionEnd.state == 1){
						System.out.println(listOfOperations.get(i)+" Committing T"+transactionEnd.id+" is added to operation list.");
						transactionEnd.operations_waiting_in_queue.add(listOfOperations.get(i));
					}
					else {
						System.out.println(listOfOperations.get(i)+" T"+transactionEnd.id+" is Committed.");
						removeAnyLocksAcquiredByAbortedTransaction(transactionEnd);
					}
						break;
			}
	
		}
	}
	
	
/* The below method is called when a transaction commit or is aborted for the first time.
 * I have tried to unlock all the items locked by aborted transaction and move the lock 
 * to other transaction operation that are eligible to acquire lock
 * 
 * 
 * 
 * 	
 */
	
	
	public void removeAnyLocksAcquiredByAbortedTransaction(Transaction transaction){
		
//		for(String ele : transaction.locked_item_by_transaction) {
//			System.out.println("*********************  "+ele);
//		}
//		System.out.println("******************** state "+transaction.state);
		
		for(String name : transaction.locked_item_by_transaction) {
			char dummy = name.charAt(3);
			Item tempItem = itemMap.get(dummy);
			
			
			if(tempItem.type_of_lock == 2) {
				
				
				if(tempItem.transactions_waiting.size() == 0) {
					tempItem.transactions_gained_lock = new LinkedList<String>();
					tempItem.type_of_lock = 0;
				}
				else {
//					if execution has reached here that means that there is no transaction id in acquired lock and move the lock to one in wait queue.
//					Since there is no operation that has lock on item, we check the waiting queue to give lock to any valid operation 
//					 
					
					Queue<String> tempQueue =new LinkedList<>();
					String ele = tempItem.transactions_waiting.poll();
					tempItem.transactions_gained_lock.add(ele);
					if(ele.charAt(0) == 'r')
						tempItem.type_of_lock = 1;
					else
						tempItem.type_of_lock = 2;
					
					Transaction eleT = transactionMap.get(ele.charAt(1)%48);
					
					tempQueue = new LinkedList<>();
					for(String operation: eleT.operations_waiting_in_queue) {
						if(!ele.equals(operation))
								tempQueue.add(operation);
					}
					eleT.operations_waiting_in_queue = new LinkedList<String>(tempQueue);
//					if there are no more instructions in waiting queue we can set the state to active for transaction.
					if(eleT.operations_waiting_in_queue.size() == 0)
						eleT.state = 0;
					eleT.locked_item_by_transaction.add(ele);
					
					
				}
				
			}
			else if(tempItem.type_of_lock == 1){
//				 remove the aborted transaction from the list which had lock on transaction.
				Queue<String> tempQueue =new LinkedList<>();
				for(String ele: tempItem.transactions_gained_lock) {				
					if(ele.charAt(1)%48 != transaction.id)
						tempQueue.add(ele);
				}
				tempItem.transactions_gained_lock = new LinkedList<String>(tempQueue);
				
//				remove the aborted transaction from the list which are waiting for lock 
				tempQueue =new LinkedList<>();
				for(String ele: tempItem.transactions_waiting) {
					if(ele.charAt(1)%48 != transaction.id)
						tempQueue.add(ele);
				}
				tempItem.transactions_waiting = new LinkedList<String>(tempQueue);
				
				if(tempItem.transactions_gained_lock.size()>0) {
					tempItem.type_of_lock = 1;
					return;
				}
				else if(tempItem.transactions_waiting.size() == 0) {
					tempItem.type_of_lock = 0;
					return;
				}
				else {
//					if execution has reached here that means that there is no transaction id in acquired lock and move the lock to one in wait queue.
//					Since there is no operation that has lock on item, we check the waiting queue to give lock to any valid operation 
					String ele = tempItem.transactions_waiting.poll();
					tempItem.transactions_gained_lock.add(ele);
					if(ele.charAt(0) == 'r')
						tempItem.type_of_lock = 1;
					else
						tempItem.type_of_lock = 2;
					
					Transaction eleT = transactionMap.get(ele.charAt(1)%48);
					
					tempQueue =new LinkedList<>();
					for(String operation: eleT.operations_waiting_in_queue) {
						if(!ele.equals(operation))
								tempQueue.add(operation);
					}
					eleT.operations_waiting_in_queue = new LinkedList<String>(tempQueue);
					if(eleT.operations_waiting_in_queue.size() == 0)
						eleT.state = 0;
					eleT.locked_item_by_transaction.add(ele);
				}
					
				

			}

			
			
		}
		
	}
	
/* The below method is used to determine if the transaction can acquire lock
 * on the requested data item. below is different type_of_lock value used;
 * 
 *  0 - item is not locked by any transaction
 *  1 - item is locked by one or more transactions in read mode
 *  2 - item is locked by one transaction in write mode
 * 
 * 
 * 
 */
	
	public int checkCanAcquireLock(Transaction transaction, Item item, char operationType) {
		
		int lockType = item.type_of_lock;
		if(lockType == 0) {
			return 0;
		}
		
		if(lockType == 1 ) {
			if(operationType == 'r')
				return 0;
			else {
				for(String tran : item.transactions_gained_lock) {
					if(transaction.id != tran.charAt(1)%48) {
						if(transaction.timestamp.before(transactionMap.get(tran.charAt(1)%48).timestamp))
							return 1;
						else
							return 2;
					}
				}
				
				return 0;
			}
		}
		
		if(lockType == 2 ) {
			for(String tran : item.transactions_gained_lock) {
				if(transaction.id != tran.charAt(1)%48) {
					if(transaction.timestamp.before(transactionMap.get(tran.charAt(1)%48).timestamp))
						return 1;
					else
						return 2;
				}
			}
		}
		
		
	return 2;	
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		update the file location here such that program reads desired file location.
		Rigorous2PL rigorous2pl = new Rigorous2PL("C:\\Users\\16825\\eclipse-spring\\Rigorous2PL\\src\\input3.txt");
		
	}

}


/*
 * Model classes to hold transaction and item information during the execution of program. 
 * 
 */
class Transaction{
	int id;
	Queue<String> operations_waiting_in_queue;
	int state;
	Queue<String> locked_item_by_transaction;
	Timestamp timestamp;
	
	public Transaction(int id) {
		this.id = id;
		state = 0;
		operations_waiting_in_queue = new LinkedList<String>();
		locked_item_by_transaction = new LinkedList<String>();
		timestamp = new Timestamp(System.currentTimeMillis());
	}
}

class Item{
	char itemName;
	int type_of_lock;
	Queue<String> transactions_gained_lock;
	Queue<String> transactions_waiting;
	
	public Item(char name) {
		this.itemName = name;
		type_of_lock = 0;
		transactions_gained_lock = new LinkedList<String>();
		transactions_waiting = new LinkedList<String>();
	}
	
}