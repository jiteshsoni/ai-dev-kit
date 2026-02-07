---
name: why-parallel-writes-no-longer-conflict
description: #
---

# Why Parallel Writes No Longer Conflict

## Overview
#

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2025-09-16
- **URL:** https://www.youtube.com/watch?v=Wt3IppIlWeE

## Tags
data-engineering

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** Wt3IppIlWeE

### Summary

#### **TL;DR**  
1. Without row-level concurrency, transactions attempting to update the same file would conflict if they targeted the same row.  
2. Row IDs and commit versions in delta tables allow multiple transactions to write to different rows without conflicts.  
3. This approach resolves conflicts by uniquely identifying each row's state, enabling parallel writes.

#### **Key Ideas**  
- Delta tables use unique row IDs and commit versions for conflict resolution.  
- Soft deletes avoid rewriting entire files, maintaining efficiency while preventing conflicts.  
- Row-level concurrency ensures transactions can write to different rows simultaneously without issues.  
- Multiple transactions can coexist as long as they target distinct rows in the same delta table.

### Full Transcript

every row inside your delta table gets a unique row ID and row commit version. So if you combine both of these concepts together you will see the benefit. Okay. So before row level concurrency what would happen? You can imagine there's a file a with four rows and deletion vectors is all zero meaning all rows are active.

Now if you delete one row essentially in the deletion vector there could be a change and you mark in the deletion vector as that row three has been deleted. So that's just soft delete or deletion vectors in action.

Now next step suppose if you have two transactions trying to update different rows. So in this example say T2 transaction trying to update row zero without rowle concurrency what would happen is if T1 has gone through then T2 who is trying to update the same file would get a conflict it would be rejected but if you add the rowle concurrency features which basically has the row ID and row commit version it would recognize that T1 has committed and T2 when it's trying to commit the file it will See something happened to this file. However, it impacted a different row. That's why I don't need to fail. That's why it can continue going on.

So that's how the concepts are coming together. Hopefully you can see that once you have the deletion vectors which gives you soft deletes, meaning you don't have to rewrite the whole park file and then you add the concept of rowle concurrency which basically gives each row a row ID and row commit version. Then you can allow two transactions or multiple transactions to write to the same delta table and they won't conflict with each other as long as they are altering different rows.

---





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
