---
title: Optimization
description: How to improve the emission profile of pipeline runs.
---

### Resource allocation

#### On similar and repeated runs
We all know it, something went wrong and now the workflow needs to be runs again.
Regarding the carbon footprint it is almost always better to allocate enough resources to avoid duplicate runs,
but if you have to repeat a run or apply a similar dataset, optimizing resource allocation can save some energy.

#### Memory
If possible, the [HTML report file](./output.md#output-report) provides a recommendation on how to adjust the allocated memory, based upon the allocated resident set size (RSS) peak of each process.
It includes a 20% buffer to avoid running out of memory.

$$
Mem_{process}^{recommended} = \lceil \max{(RSS_{process}) } \cdot 1.2 ) \rceil_{GB}
$$