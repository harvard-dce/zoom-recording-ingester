## series.xml example

```
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<dublincore xmlns="http://www.opencastproject.org/xsd/1.0/dublincore/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:oc="http://www.opencastproject.org/matterhorn/">
    <dcterms:creator>Harvard Extension School</dcterms:creator>  <!-- (or Summer) -->
    <dcterms:subject>Test E-19999</dcterms:subject> <!-- (course number) -->
    <dcterms:publisher>Harvard University, DCE</dcterms:publisher>
    <dcterms:title>Test Fall 2018 -DO NOT USE -DELETEME</dcterms:title>
    <dcterms:contributor>Instructor H. Faculty</dcterms:contributor> <!-- (instructor) -->
    <dcterms:created>2017-08-15T04:00:00Z</dcterms:created>
    <dcterms:description>http://www.ext.harvard.edu</dcterms:description>;  <!-- (course web site) -->
    <dcterms:language>eng</dcterms:language>
    <oc:annotation>false</oc:annotation>
    <dcterms:identifier>20180119999</dcterms:identifier>
</dublincore>
```

## episode.xml example

```
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<dublincore xmlns="http://www.opencastproject.org/xsd/1.0/dublincore/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <dcterms:creator>Instructor H. Faculty</dcterms:creator> <!-- (instructor) -->
    <dcterms:type>L01</dcterms:type> <!-- (type number L,P,S99 ---- AUTH!!!!!) -->
    <dcterms:extent xsi:type="dcterms:ISO8601">PT2H59M41.603S</dcterms:extent>
    <dcterms:isPartOf>20180119999</dcterms:isPartOf> <!-- (series) -->
    <dcterms:license>Creative Commons 3.0: Attribution-NonCommercial-NoDerivs</dcterms:license>
    <dcterms:temporal xsi:type="dcterms:W3CDTF">2018-12-13T23:30:00Z</dcterms:temporal> <!-- (start) -->
    <dcterms:publisher>some-producer@harvard.edu</dcterms:publisher> <!-- (producer email) -->
    <dcterms:title>Lecture</dcterms:title> <!-- (title) -->
    <dcterms:contributor>producer</dcterms:contributor> <!-- (producer name) -->
    <dcterms:created xsi:type="dcterms:W3CDTF">2018-12-13T23:30:00Z</dcterms:created>
    <dcterms:recordDate>2017-06-19</dcterms:recordDate>
</dublincore>
````
