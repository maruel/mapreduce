Yet another mapreduce library
=============================

  - As simple as it can be
  - Map cacheability that can be serialized as-is
  - Single process

The rationale for writting it is to use it as a frontend when harvesting data
from a slow web server, so that the data can be cached across executions.

[![GoDoc](https://godoc.org/github.com/maruel/mapreduce?status.svg)](https://godoc.org/github.com/maruel/mapreduce)
[![Build Status](https://travis-ci.org/maruel/mapreduce.svg?branch=master)](https://travis-ci.org/maruel/mapreduce)
[![Coverage Status](https://img.shields.io/coveralls/maruel/mapreduce.svg)](https://coveralls.io/r/maruel/mapreduce?branch=master)
