package com.twitter.elephantbird.util;

import com.google.common.collect.ImmutableMap;
import com.twitter.elephantbird.util.Strings;

import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Tests for Strings.
 *
 * Largely taken from the defunct rogueweb project at
 * <link>http://code.google.com/p/rogueweb/</link>
 * Original author Anthony Eden.
 */
public class TestStrings {

  private static final Map<String, String> singularToPlural_ = ImmutableMap.<String, String>builder()
                                                                           .put("search", "searches")
                                                                           .put("switch", "switches")
                                                                           .put("fix", "fixes")
                                                                           .put("box", "boxes")
                                                                           .put("process", "processes")
                                                                           .put("address", "addresses")
                                                                           .put("case", "cases")
                                                                           .put("stack", "stacks")
                                                                           .put("wish", "wishes")
                                                                           .put("fish", "fish")
                                                                           .put("category", "categories")
                                                                           .put("query", "queries")
                                                                           .put("ability", "abilities")
                                                                           .put("agency", "agencies")
                                                                           .put("movie", "movies")
                                                                           .put("company", "companies")
                                                                           .put("archive", "archives")
                                                                           .put("index", "indices")
                                                                           .put("wife", "wives")
                                                                           .put("safe", "saves")
                                                                           .put("half", "halves")
                                                                           .put("move", "moves")
                                                                           .put("salesperson", "salespeople")
                                                                           .put("person", "people")
                                                                           .put("spokesman", "spokesmen")
                                                                           .put("man", "men")
                                                                           .put("woman", "women")
                                                                           .put("basis", "bases")
                                                                           .put("diagnosis", "diagnoses")
                                                                           .put("datum", "data")
                                                                           .put("medium", "media")
                                                                           .put("analysis", "analyses")
                                                                           .put("node_child", "node_children")
                                                                           .put("child", "children")
                                                                           .put("experience", "experiences")
                                                                           .put("day", "days")
                                                                           .put("comment", "comments")
                                                                           .put("foobar", "foobars")
                                                                           .put("newsletter", "newsletters")
                                                                           .put("old_news", "old_news")
                                                                           .put("news", "news")
                                                                           .put("series", "series")
                                                                           .put("species", "species")
                                                                           .put("quiz", "quizzes")
                                                                           .put("perspective", "perspectives")
                                                                           .put("ox", "oxen")
                                                                           .put("photo", "photos")
                                                                           .put("buffalo", "buffaloes")
                                                                           .put("tomato", "tomatoes")
                                                                           .put("dwarf", "dwarves")
                                                                           .put("elf", "elves")
                                                                           .put("information", "information")
                                                                           .put("equipment", "equipment")
                                                                           .put("bus", "buses")
                                                                           .put("status", "statuses")
                                                                           .put("status_code", "status_codes")
                                                                           .put("mouse", "mice")
                                                                           .put("louse", "lice")
                                                                           .put("house", "houses")
                                                                           .put("octopus", "octopi")
                                                                           .put("virus", "viri")
                                                                           .put("alias", "aliases")
                                                                           .put("portfolio", "portfolios")
                                                                           .put("vertex", "vertices")
                                                                           .put("matrix", "matrices")
                                                                           .put("axis", "axes")
                                                                           .put("testis", "testes")
                                                                           .put("crisis", "crises")
                                                                           .put("rice", "rice")
                                                                           .put("shoe", "shoes")
                                                                           .put("horse", "horses")
                                                                           .put("prize", "prizes")
                                                                           .put("edge", "edges")
                                                                           .build();

  private static final Map<String, String> mixtureToTitleCase_ = ImmutableMap.<String, String>builder()
                                                                             .put("active_record", "Active Record")
                                                                             .put("ActiveRecord", "Active Record")
                                                                             .put("action web service", "Action Web Service")
                                                                             .put("Action Web Service", "Action Web Service")
                                                                             .put("Action web service", "Action Web Service")
                                                                             .put("actionwebservice", "Actionwebservice")
                                                                             .put("Actionwebservice", "Actionwebservice")
                                                                             .build();

  private static final Map<String, String> camelToUnderscore_ = ImmutableMap.<String, String>builder()
                                                                            .put("Product", "product")
                                                                            .put("SpecialGuest", "special_guest")
                                                                            .put("ApplicationController", "application_controller")
                                                                            .put("Area51Controller", "area51_controller")
                                                                            .put("InnerClass$Test", "inner_class__test")
                                                                            .build();

  private static final Map<String, String> camelToUnderscoreWithoutReverse_ = ImmutableMap.<String, String>builder()
                                                                                          .put("HTMLTidy", "html_tidy")
                                                                                          .put("HTMLTidyGenerator", "html_tidy_generator")
                                                                                          .put("FreeBSD", "free_bsd")
                                                                                          .put("HTML", "html")
                                                                                          .build();

  private static final Map<String, String> camelWithPackageToUnderscoreWithSlash_ = ImmutableMap.<String, String>builder()
                                                                                                .put("admin.Product", "admin/product")
                                                                                                .put("users.commission.Department", "users/commission/department")
                                                                                                .put("usersSection.CommissionDepartment", "users_section/commission_department")
                                                                                                .build();

  private static final Map<String, String> classNameToForeignKeyWithUnderscore_ = ImmutableMap.<String, String>builder()
                                                                                              .put("Person", "person_id")
                                                                                              .put("application.billing.Account", "account_id")
                                                                                              .build();

  private static final Map<String, String> classNameToTableName_ = ImmutableMap.<String, String>builder()
                                                                               .put("PrimarySpokesman", "primary_spokesmen")
                                                                               .put("NodeChild", "node_children")
                                                                               .build();

  private static final Map<String, String> underscoreToHuman_ = ImmutableMap.<String, String>builder()
                                                                            .put("employee_salary", "Employee salary")
                                                                            .put("employee_id", "Employee")
                                                                            .put("underground", "Underground")
                                                                            .build();

  private static final Map<Integer, String> ordinalNumbers_ = ImmutableMap.<Integer, String>builder()
                                                                          .put(0, "0th")
                                                                          .put(1, "1st")
                                                                          .put(2, "2nd")
                                                                          .put(3, "3rd")
                                                                          .put(4, "4th")
                                                                          .put(5, "5th")
                                                                          .put(6, "6th")
                                                                          .put(7, "7th")
                                                                          .put(8, "8th")
                                                                          .put(9, "9th")
                                                                          .put(10, "10th")
                                                                          .put(11, "11th")
                                                                          .put(12, "12th")
                                                                          .put(13, "13th")
                                                                          .put(14, "14th")
                                                                          .put(20, "20th")
                                                                          .put(21, "21st")
                                                                          .put(22, "22nd")
                                                                          .put(23, "23rd")
                                                                          .put(24, "24th")
                                                                          .put(100, "100th")
                                                                          .put(101, "101st")
                                                                          .put(102, "102nd")
                                                                          .put(103, "103rd")
                                                                          .put(104, "104th")
                                                                          .put(110, "110th")
                                                                          .put(1000, "1000th")
                                                                          .put(1001, "1001st")
                                                                          .put(10013, "10013th")
                                                                          .build();

  private static final Map<String, String> underscoresToDashes_ = ImmutableMap.<String, String>builder()
                                                                              .put("street", "street")
                                                                              .put("street_address", "street-address")
                                                                              .put("person_street_address", "person-street-address")
                                                                              .build();

  private static final Map<String, String> underscoreToLowerCamel_ = ImmutableMap.<String, String>builder()
                                                                                 .put("product","product")
                                                                                 .put("special_guest","specialGuest")
                                                                                 .put("application_controller","applicationController")
                                                                                 .put("area51_controller","area51Controller")
                                                                                 .build();

  @Test
  public void testPluralizePlurals() {
    assertEquals("plurals", Strings.pluralize("plurals"));
    assertEquals("Plurals", Strings.pluralize("Plurals"));
  }

  @Test
  public void testPluralize() {
    for (Map.Entry<String, String> entry : singularToPlural_.entrySet()) {
      String singular = entry.getKey();
      String plural = entry.getValue();
      assertEquals(plural, Strings.pluralize(singular));
      assertEquals(Strings.capitalize(plural),
          Strings.pluralize(Strings.capitalize(singular)));
    }
  }

  @Test
  public void testSingularize() {
    for (Map.Entry<String, String> entry : singularToPlural_.entrySet()) {
      String singular = entry.getKey();
      String plural = entry.getValue();
      assertEquals(singular, Strings.singularize(plural));
      assertEquals(Strings.capitalize(singular),
          Strings.singularize(Strings.capitalize(plural)));
    }
  }

  @Test
  public void testTitleize() {
    for (Map.Entry<String, String> entry : mixtureToTitleCase_.entrySet()) {
      assertEquals(entry.getValue(), Strings.titleize(entry.getKey()));
    }
  }

  @Test
  public void testCamelize() {
    for (Map.Entry<String, String> entry : camelToUnderscore_.entrySet()) {
      assertEquals(entry.getKey(), Strings.camelize(entry.getValue()));
    }
  }

  @Test
  public void testUnderscore() {
    for (Map.Entry<String, String> entry : camelToUnderscore_.entrySet()) {
      assertEquals(entry.getValue(), Strings.underscore(entry.getKey()));
    }
    for (Map.Entry<String, String> entry : camelToUnderscoreWithoutReverse_.entrySet()) {
      assertEquals(entry.getValue(), Strings.underscore(entry.getKey()));
    }
  }

  @Test
  public void testCamelizeWithPackage() {
    for (Map.Entry<String, String> entry : camelWithPackageToUnderscoreWithSlash_.entrySet()) {
      assertEquals(entry.getKey(), Strings.camelize(entry.getValue()));
    }
  }

  @Test
  public void testUnderscoreWithSlashes() {
    for (Map.Entry<String, String> entry : camelWithPackageToUnderscoreWithSlash_.entrySet()) {
      assertEquals(entry.getValue(), Strings.underscore(entry.getKey()));
    }
  }

  @Test
  public void testDepackage() {
    assertEquals("Account", Strings.depackage("application.billing.Account"));
  }

  @Test
  public void testForeignKey() {
    for (Map.Entry<String, String> entry : classNameToForeignKeyWithUnderscore_.entrySet()) {
      assertEquals(entry.getValue(), Strings.foreignKey(entry.getKey()));
    }
  }

  @Test
  public void testTableize() {
    for (Map.Entry<String, String> entry : classNameToTableName_.entrySet()) {
      assertEquals(entry.getValue(), Strings.tableize(entry.getKey()));
    }
  }

  @Test
  public void testClassify() {
    for (Map.Entry<String, String> entry : classNameToTableName_.entrySet()) {
      assertEquals(entry.getKey(), Strings.classify(entry.getValue()));
    }
  }

  @Test
  public void testHumanize() {
    for (Map.Entry<String, String> entry : underscoreToHuman_.entrySet()) {
      assertEquals(entry.getValue(), Strings.humanize(entry.getKey()));
    }
  }

  @Test
  public void testOrdinal() {
    for (Map.Entry<Integer, String> entry : ordinalNumbers_.entrySet()) {
      assertEquals(entry.getValue(), Strings.ordinalize(entry.getKey()));
      assertEquals(entry.getValue(), Strings.ordinalize(entry.getKey()));
    }
  }

  @Test
  public void testDasherize() {
    for (Map.Entry<String, String> entry : underscoresToDashes_.entrySet()) {
      assertEquals(entry.getValue(), Strings.dasherize(entry.getKey()));
    }
  }

  @Test
  public void testUnderscoredAsReverseOfDasherize() {
    for (Map.Entry<String, String> entry : underscoresToDashes_.entrySet()) {
      assertEquals(entry.getKey(), Strings.underscore(Strings.dasherize(entry.getKey())));
    }
  }

  @Test
  public void testUnderscoreToLowerCamel() {
    for (Map.Entry<String, String> entry : underscoreToLowerCamel_.entrySet()) {
      assertEquals(entry.getValue(), Strings.camelize(entry.getKey(), true));
    }
  }

  @Test
  public void testCapitalize() {
    assertEquals("Foo bar baz", Strings.capitalize("foo bar baz"));
  }

  @Test
  public void testClassNameToTableName() {
    assertEquals("companies", Strings.tableize("com.aetrion.activerecord.fixture.Companies"));
  }

}
