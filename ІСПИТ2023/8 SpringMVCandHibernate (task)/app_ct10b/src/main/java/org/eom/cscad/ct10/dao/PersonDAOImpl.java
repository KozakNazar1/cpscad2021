package org.eom.cscad.ct10.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		createIfNotExist();
		listPersons();
	}
	
	private void createIfNotExist() {
		Connection conn = null;		
		try {
			String url = "jdbc:mysql://localhost:3306/sakila";
			conn = DriverManager.getConnection(url, "root", "12345");
			Statement stmt = conn.createStatement();
			String sqlStr = "CREATE TABLE IF NOT EXISTS person (" 
			                + " id INT(11) NOT NULL AUTO_INCREMENT,"
			                + " name VARCHAR(255) DEFAULT NULL,"
			                + " country VARCHAR(255) DEFAULT NULL,"														
			                + " PRIMARY KEY  (id)) ENGINE=INNODB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;";								
							

			System.out.println(sqlStr);
			stmt.executeUpdate(sqlStr);

			conn.close();
		} catch (Exception e) {
			System.err.println("Got an exception! ");
			System.err.println(e.getMessage());
        }finally{
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}   		
        }
	}
	@Override
	public void addPerson(Person p) {		
            String sqlStr = 
    	    		"INSERT INTO person (name, country)" + " "
    	    	    		+ "VALUES ('" + p.getName() + "', '" + p.getCountry() + "');";   		
 
        Connection conn = null;        
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();         

            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());

            
    		//System.out.println(sqlStr);                               	
    		stmt.executeUpdate(sqlStr);
             
    		logger.info("Person saved successfully, Person Details="+p);    		
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }finally{
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}   		
        }			
		
	}

	@Override
	public void updatePerson(Person p) {
            String sqlStr = 
    	    		"UPDATE person" + " "
    	    	    		+ "SET name ='" + p.getName() + "', country = '" + p.getCountry() + "'" + " "
    	    	    		+ "WHERE id = " + Integer.toString(p.getId()) +  ";";     
		
        Connection conn = null;        
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();	    	    		
           
            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());
           
    		//System.out.println(sqlStr);             		
    		stmt.executeUpdate(sqlStr); 
    		
    		logger.info("Person updated successfully, Person Details="+p);    		
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }finally{
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}   		
        }							
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Person> listPersons() {
				
		personsList = new ArrayList<Person>();		
		Connection conn = null;			
		
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            conn = DriverManager.getConnection(url,"root","12345");
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
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }finally{
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}   		
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
		
		Connection conn = null;
        try {
            String url = "jdbc:mysql://localhost:3306/sakila";
            conn = DriverManager.getConnection(url,"root","12345");
            Statement stmt = conn.createStatement();

    		String sqlStr = "DELETE FROM person" + " "
    		+ "WHERE id = " + Integer.toString(id) +  ";"; 
           
            List<Field> allFields = Arrays.asList(Person.class.getDeclaredFields());
            //System.out.println(sqlStr);                         		
    		stmt.executeUpdate(sqlStr); 
    		
    		logger.info("Person deleted successfully, person details="+ p);
        } catch (Exception e) {       	
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }finally{
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}   		
        }		
		
		
	}

}
