package com.twitter.elephantbird.pig.piggybank;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.twitter.elephantbird.pig.piggybank.JsonToPigParser.Features.DETECT_JSON_IN_ALL_STRINGS;
import static com.twitter.elephantbird.pig.piggybank.JsonToPigParser.Features.NESTED_JSON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestJsonToPigParser {
    private static final ObjectMapper mapper = new ObjectMapper();
    private Person john;
    private String johnJsonString;

    @Before
    public void setup() throws IOException {
        john = new Person("john");

        Person jane = new Person("jane");
        john.setSpouse(jane);
        Map details = new HashMap();
        details.put("age", 29);
        details.put("likes", Arrays.asList("the letter 'J'", "cookies"));
        jane.setDetailsString(mapper.writeValueAsString(details));
        jane.setDetails(details);

        Person jimmy = new Person("jimmy");
        Person jenny = new Person("jenny");

        john.addChildren(jimmy, jenny);
        johnJsonString = mapper.writeValueAsString(john);
    }

    @Test
    public void returnNullForInvalidJson(){
        JsonToPigParser parser = new JsonToPigParser();
        assertNull(parser.toPigMap("..."));
    }

    @Test
    public void wrapNonObjectsIntoMap() throws ExecException {
        JsonToPigParser parser = new JsonToPigParser();
        Map map = parser.toPigMap("100");
        assertEquals("100",map.get("value"));

        map = parser.toPigMap("\"a\"");
        assertEquals("a", map.get("value"));

        map = parser.toPigMap("[\"a\"]");
        DataBag bag = (DataBag)map.get("value");
        Tuple tuple = bag.iterator().next();
        assertEquals("a", tuple.get(0));
    }

    @Test
    public void supportWrappedJsonString(){
        JsonToPigParser parser = new JsonToPigParser();
        String doubleJson = "\"{\\\"a\\\":22}\"";
        Map map = parser.toPigMap(doubleJson);
        assertEquals("22", map.get("a"));
    }

    @Test
    public void convertSimpleTopLevelFields(){
        JsonToPigParser nestedConverter = new JsonToPigParser();
        Map<String,Object> pigJohn = nestedConverter.toPigMap(johnJsonString);
        assertSimpleFieldsMatch(john, pigJohn);
    }

    @Test
    public void convertNestedMaps() throws ExecException {
        JsonToPigParser nestedConverter = new JsonToPigParser(NESTED_JSON);
        Map<String,Object> pigJohn = nestedConverter.toPigMap(johnJsonString);

        Person spouse = john.getSpouse();
        Map spouseMap = (Map)pigJohn.get("spouse");
        assertSimpleFieldsMatch(spouse, spouseMap);
    }

    @Test
    public void convertMultiLevelNesting() throws ExecException {
        JsonToPigParser nestedConverter = new JsonToPigParser(NESTED_JSON);
        Map<String,Object> pigJohn = nestedConverter.toPigMap(johnJsonString);

        Person spouse = john.getSpouse();
        Map spouseMap = (Map)pigJohn.get("spouse");

        Map<String,Object> detailsMap = (Map<String,Object>)spouseMap.get("details");
        assertEquals(String.valueOf(spouse.getDetails().get("age")), detailsMap.get("age"));

        DataBag likesBag = (DataBag)detailsMap.get("likes");
        List expectedLikesList = (List)spouse.getDetails().get("likes");
        assertEquals(expectedLikesList.size(), likesBag.size());

        Iterator likesBagIter = likesBag.iterator();
        int i=0;
        while(likesBagIter.hasNext()){
            Tuple likeTuple = (Tuple)likesBagIter.next();
            assertEquals(expectedLikesList.get(i++), likeTuple.get(0));
        }

        DataBag childrenBag = (DataBag)pigJohn.get("children");
        List<Person> expectedChildrenList = john.getChildren();
        assertEquals(expectedChildrenList.size(), childrenBag.size());

        Iterator childrenBagIter = childrenBag.iterator();
        i=0;
        while(childrenBagIter.hasNext()){
            Tuple childTuple = (Tuple)childrenBagIter.next();
            assertSimpleFieldsMatch(expectedChildrenList.get(i++), (Map) childTuple.get(0));
        }

    }

    @Test
    public void supportFlatteningNestedJsonStructures() throws IOException {
        JsonToPigParser flatteningConverter = new JsonToPigParser();
        Map<String,Object> pigJohn = flatteningConverter.toPigMap(johnJsonString);
        assertSimpleFieldsMatch(john, pigJohn);
        assertEquals(mapper.writeValueAsString(john.getSpouse()), pigJohn.get("spouse"));
        assertEquals(mapper.writeValueAsString(john.getChildren()), pigJohn.get("children"));
    }

    @Test
    public void supportDetectingEmbeddedJsonInStringFieldValues(){
        JsonToPigParser parserWithEmbedDetection = new JsonToPigParser(DETECT_JSON_IN_ALL_STRINGS);
        Map<String,Object> pigJohn = parserWithEmbedDetection.toPigMap(johnJsonString);
        assertSimpleFieldsMatch(john, pigJohn);
        Map spouseMap = (Map)pigJohn.get("spouse");
        Map spouseDetailsString = (Map)spouseMap.get("detailsString");
        Map spouseDetails = (Map)spouseMap.get("details");
        assertEquals(spouseDetails, spouseDetailsString);
    }

    @Test
    public void parseNullStringAsNull(){
        JsonToPigParser parser = new JsonToPigParser();
        assertNull(parser.toPigMap("null"));
    }

    private void assertSimpleFieldsMatch(Person person, Map pigMap) {
        assertEquals(String.valueOf(person.getId()), pigMap.get("id"));
        assertEquals(person.getName(), pigMap.get("name"));
        assertEquals(person.getDetailsString(), pigMap.get("detailsString"));
    }
}

class Person{
    private static int nextId = 1;

    private int id;
    private String name;
    private List<Person> children;
    private Person spouse;
    private Map details;
    private String detailsString;

    Person(String name){
        id = nextId++;
        this.name = name;
        children = new ArrayList<Person>();
    }

    void addChildren(Person...children){
        this.children.addAll(Arrays.asList(children));
    }

    void setSpouse(Person spouse){
        this.spouse = spouse;
    }

    public void setDetails(Map details) {
        this.details = details;
    }

    public void setDetailsString(String infoString) {
        this.detailsString = infoString;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<Person> getChildren() {
        return children;
    }

    public Person getSpouse() {
        return spouse;
    }

    public Map getDetails() {
        return details;
    }

    public String getDetailsString() {
        return detailsString;
    }
}
