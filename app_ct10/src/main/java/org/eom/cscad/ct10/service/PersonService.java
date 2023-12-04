package org.eom.cscad.ct10.service;

import java.util.List;

import org.eom.cscad.ct10.model.Person;

public interface PersonService {

	public void addPerson(Person p);
	public void updatePerson(Person p);
	public List<Person> listPersons();
	public Person getPersonById(int id);
	public void removePerson(int id);
	
}
