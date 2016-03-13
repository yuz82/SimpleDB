/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;

/**
 *
 * @author yuezhao
 */
public class SimpleDB {

    /**
     * @param args the command line arguments
     */
    private final HashMap<String,Integer> database = new HashMap<>();
    private final HashMap<Integer,Integer> valueCount = new HashMap<>(); //numequalto
    
    private Stack<Transaction> pendingTransaction = new Stack<>();
    
    private class Transaction {
        private HashMap<String,Integer> tmp = new HashMap<>();

        public void commit(){ 
            for(Map.Entry<String, Integer> entry : tmp.entrySet()){
                database.put(entry.getKey(), entry.getValue());
            }
        }
        
    }
    
    /**
     * set
     * 
     * Set the variable name to the value value. Neither variable names nor values will contain spaces.
     * @param name
     * @param value 
     */
    public void set(String name, Integer value){
        Integer previous = this.get(name);
        if(value!=null){ increaseCount(value); }
        if(previous!=null){ decreaseCount(previous); }
        if(pendingTransaction.empty()){ //not in a transaction
            database.put(name, value);
        }else{
            pendingTransaction.peek().tmp.put(name, value);
        }
    }
    
    public void increaseCount(int value) {
        if(valueCount.containsKey(value)){
            valueCount.put(value, valueCount.get(value)+1);
        }else{
            valueCount.put(value, 1);
        }
    }
    
    public void decreaseCount(int value){
        if(valueCount.containsKey(value) && valueCount.get(value) > 0){
            valueCount.put(value, valueCount.get(value)-1);
        }
    }
    
    /**
     * get
     * 
     * Print out the value of the variable name, or NULL if that variable is not set.
     * @param name 
     */
    public Integer get(String name){
        if(!pendingTransaction.empty()){
            for(int i=pendingTransaction.size()-1;i>-1;i--){
                if(pendingTransaction.get(i).tmp.containsKey(name)){
                    if(pendingTransaction.get(i).tmp.get(name)!=null){
                        return pendingTransaction.get(i).tmp.get(name);
                    }else{
                        return null;
                    }
                }
            }
        }
        if(database.containsKey(name) && database.get(name)!=null){
            return database.get(name);
        }
        return null;
    }
    
    /**
     * unset
     * 
     * Unset the variable name, making it just like that variable was never set.
     * @param name 
     */
    public void unset(String name){
        set(name, null);
    }
    
    /**
     * numequalto
     * 
     * Print out the number of variables that are currently set to value. 
     * If no variables equal that value, print 0.
     * @param value 
     */
    public void numequalto(int value){
        if(valueCount.containsKey(value) && valueCount.get(value)!=null){
            System.out.println(valueCount.get(value));
            return;
        }
        System.out.println(0);
    }
    
    /**
     * end
     * 
     * Exit the program. Your program will always receive this as its last command.
     */
    public void end(){
        System.exit(0);
    }
    
    /**
     * begin
     * 
     * Open a new transaction block. 
     * Transaction blocks can be nested; a BEGIN can be issued inside of an existing block.
     */
    public void begin(){
        pendingTransaction.push(new Transaction());
    }
    
    /**
     * rollback
     * 
     * Undo all of the commands issued in the most recent transaction block, and close the block. 
     * Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.
     */
    public void rollback(){
        if(pendingTransaction.empty()){
            System.out.println("NO TRANSACTION");
        }else{
            Transaction t = pendingTransaction.pop();
            for(Map.Entry<String, Integer> entry : t.tmp.entrySet()){
                if(entry.getValue()!=null) { decreaseCount(entry.getValue()); }
                if(get(entry.getKey())!=null){
                    increaseCount(get(entry.getKey()));
                }
            }
        }
    }
    
    /**
     * commit
     * 
     * Close all open transaction blocks, permanently applying the changes made in them. 
     * Print nothing if successful, or print NO TRANSACTION if no transaction is in progress.
     */
    public void commit(){
        if(pendingTransaction.empty()){
            System.out.println("NO TRANSACTION");
        }else{
            pendingTransaction.pop().commit();
        }
        pendingTransaction = new Stack<Transaction>();
    }
    
    public void command(String line){
        String[] strs = line.split(" ");
        switch(strs[0]){
            case "SET":
                this.set(strs[1], Integer.parseInt(strs[2]));
                break;
            case "GET":
                Integer res = this.get(strs[1]);
                if(res==null){
                System.out.println("NULL");
                }else{
                    System.out.println(res);
                }
                break;
            case "UNSET":
                this.unset(strs[1]);
                break;
            case "NUMEQUALTO":
                this.numequalto(Integer.parseInt(strs[1]));
                break;
            case "END":
                this.end();
                break;
            case "BEGIN":
                this.begin();
                break;
            case "ROLLBACK":
                this.rollback();
                break;
            case "COMMIT":
                this.commit();
                break;
            default:
                System.out.println("NOT VALID COMMAND");
        }
    }
    
    public static void main(String[] args){
        SimpleDB simpleDB = new SimpleDB();
        //read commands
        if(args.length==0){
            Scanner sc = new Scanner(System.in);
            while(sc.hasNextLine()){
                simpleDB.command(sc.nextLine());
            }
        }else {
            String fileName = args[0];
            try(BufferedReader br = new BufferedReader(new FileReader(fileName))){
                String line = null;
                while((line = br.readLine()) != null){
                    simpleDB.command(line);
                }
            }catch(FileNotFoundException ex){
                System.out.println("Unable to open '"+fileName+"'");
            }catch(IOException ex){
                System.out.println("Unable to read '"+fileName+"'");
            }
        }
    }
    
}
