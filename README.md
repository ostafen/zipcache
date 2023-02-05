# zipcache
An in-memory compressed cache

# Why?

Caching many items in memory can considerably increase the space requirements of an application. A simple way to mitigate this problem is to compress each cache entry using some compression algorithm (deflate, gzip, etc...). Howewer this approach is not effective when the average size of each entry is relatively small.

## Layout

<p align="center">
  <img src="./diagram.png" />
</p>