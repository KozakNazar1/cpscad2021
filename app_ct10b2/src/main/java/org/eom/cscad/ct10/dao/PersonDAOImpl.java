package org.eom.cscad.ct10.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Repository;


import org.eom.cscad.ct10.model.Person;

import java.sql.*;
import java.lang.reflect.Field;

//@Repository
@SuppressWarnings("unused")
public class PersonDAOImpl implements PersonDAO {
	
	private static final Logger logger = LoggerFactory.getLogger(PersonDAOImpl.class);


	List<Person> personsList;
	
	private PersonDAOImpl(){
		listPersons();
	}
	
	private Map<String, Object> getFieldRecords(Person p){		
		Map<String, Object> fieldNameRecords = new HashMap<String, Object>();			
        for (Field field : Arrays.asList(Person.class.getDeclaredFields())) {
        	String fieldStr = field.toString();            	    	
        	field.setAccessible(true);  
        	try {
				fieldNameRecords.put(fieldStr.substring(fieldStr.lastIndexOf('.') + 1),
						field.get(p));
			} catch (IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	field.setAccessible(false);   	
      	}          	
		return fieldNameRecords;
	}	

	@Override
	public void addPerson(Person p) {	
		String fildNamessStr = "";
		String fildValuesStr = "";		
		Map<String, Object> fieldNameRecords = getFieldRecords(p);
        for (Map.Entry<String, Object> pair : fieldNameRecords.entrySet()) {
    		if(fildNamessStr.length() != 0) {
        		fildNamessStr += ",";
        		fildValuesStr += ",";	     			
    		}        	
    		fildNamessStr += pair.getKey();
    		fildValuesStr += "'" + pair.getValue().toString() + "'";	 
         	
      	}		
	
        String sqlStr = 
	    		"INSERT INTO person (" + fildNamessStr + ")" + " "
	    	    		+ "VALUES (" + fildValuesStr + ");";  		
 
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            Connection conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();         

            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());

            
    		//System.out.println(sqlStr);                               	
    		stmt.executeUpdate(sqlStr);
             
    		logger.info("Person saved successfully, Person Details="+p);    		

            conn.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }			
		
	}

	@Override
	public void updatePerson(Person p) {	
		String fildsStr = "";		
		Map<String, Object> fieldNameRecords = getFieldRecords(p);
        for (Map.Entry<String, Object> pair : fieldNameRecords.entrySet()) {
        	if(pair.getKey().equals("id")) {
        		continue;
        	}
    		if(fildsStr.length() != 0) {
    			fildsStr += ",";    			
    		}        	
    		fildsStr += pair.getKey() + " = " + "'" + pair.getValue().toString() + "'";         
      	} 			                 
	
        String sqlStr = 
	    		"UPDATE person" + " "
	    	    		+ "SET " + fildsStr + " "
	    	    		+ "WHERE id = " + Integer.toString(p.getId()) +  ";";      
		
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            Connection conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();	    	    		
           
            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());
           
    		//System.out.println(sqlStr);             		
    		stmt.executeUpdate(sqlStr); 
    		
    		logger.info("Person updated successfully, Person Details="+p);    		

            conn.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }							
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Person> listPersons() {
				
		personsList = new ArrayList<Person>();		
				
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            Connection conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();
            String sqlStr = 
            		"SELECT * FROM person";              
            ResultSet rs= stmt.executeQuery(sqlStr);
           
            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());            

            while ( rs.next() ) {
           	
            	
            	Person p = new Person();
                for (Field field : allFields) {
                	String fieldStr = field.toString();            	    	
                	String last = fieldStr.substring(fieldStr.lastIndexOf('.') + 1);                	               	
                	if(field.getGenericType().toString().equals(String.class.toString())){ // .getGenericType()
                    	field.setAccessible(true);                	
                    	field.set(p, rs.getString(last));
                    	field.setAccessible(false);                  	
                	}
                	else{ // int for others
                    	field.setAccessible(true);                	
                    	field.set(p, Integer.parseInt(rs.getString(last)));
                    	field.setAccessible(false); 
                	}
                              	
              	}                       	
            	
                personsList.add(p);
                logger.info("Person List::"+ p);          	
            	
            }
            
            conn.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }		
		
		return personsList;
	}

	@Override
	public Person getPersonById(int id) {	
		for(Person p: personsList) {
			if(p.getId() == id) {
				logger.info("Person loaded successfully, Person details="+p);
				return p;
			}
		}
		
		return new Person(); // impossible code
		
	}

	@Override
	public void removePerson(int id) {
		Person p = getPersonById(id);
		
		if(p == null) {
			return;
		}
		
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            Connection conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();

    		String sqlStr = "DELETE FROM person" + " "
    		+ "WHERE id = " + Integer.toString(id) +  ";"; 
           
            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());
            //System.out.println(sqlStr);                         		
    		stmt.executeUpdate(sqlStr); 
    		
    		logger.info("Person deleted successfully, person details="+ p);

            conn.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }		
		
		
	}

}
