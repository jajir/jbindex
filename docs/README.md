![example workflow](https://github.com/jajir/jbindex/actions/workflows/maven.yml/badge.svg)

# jbindex

Goal is to provide easy to use key value map for billions of records using just one directory and some space.

It's simple fast index. Work with index should be split into phases of:

* Writing data to index. All data that should be stored in index should be send to index.
* Building index. In this phase data are organized for fast access.
* Search through index. In this phase it's not possible to alter data in index.

Index is not thread safe.

## Useful links

* [Project versioning and how to release snapshot and new version](release.md)
* [Segment implementation details](segment.md)

## Basic work with index

Index could be in following states:

![Index methods](index-class.png)

Index should be created with builder, which make index instance. For example:

```java
final Index<Integer, String> index = Index.<Integer, String>builder()
        .withDirectory(directory)
        .withKeyClass(Integer.class)
        .withValueClass(String.class)
        .build();
```

![Index states](index-state-diagram.png)

Interruption of process of writing data to index could lead to corruption of entire index.

## Development

Mockito requires reflective access to non-public parts in a Java module. It could be manually open by passing following parameter as jvm parameter:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
```

## How to get segment disk size

On apple try:

```
diskutil  info /Volumes/LaCie
```
