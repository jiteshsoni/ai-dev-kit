---
name: offsets-inclusive-start-exclusive-end
description: ### Full Transcript

Another thing I wanted to talk about just some small detail is the offset folder actually contains information about till which o...
---

# Offsets: inclusive start, exclusive end

## Overview
### Full Transcript

Another thing I wanted to talk about just some small detail is the offset folder actually contains information about till which offset it's trying to process. Example say let's say in our offset n we wrote start offset as 100 and end offset as 200. So when we process the data the end offset is exclusive. So we are going to process 100 to 199 and when the next batch runs say n plus2 batch runs we're going to process from 200 to 299 assuming we are processing 100 offsets.

Just a small level of detail that the front part is inclusive and the end part is exclusive. If you're looking for more details of what exactly is stored in the checkpoint, I have a whole video on that. I'll leave a link to that in the description of this blog. There I talk about each of these uh diffe

## Video Information
- **Author:** Canadian Data Guy
- **Date:** 2026-01-27
- **URL:** https://www.youtube.com/watch?v=NwvurfAk6lg

## Tags
ai, checkpoints

## Key Topics Covered
- Spark Streaming and Delta Lake concepts
- Practical implementation guidance
- Production best practices

## Content Summary

  
**Video ID:** NwvurfAk6lg

### Summary

### Full Transcript

Another thing I wanted to talk about just some small detail is the offset folder actually contains information about till which offset it's trying to process. Example say let's say in our offset n we wrote start offset as 100 and end offset as 200. So when we process the data the end offset is exclusive. So we are going to process 100 to 199 and when the next batch runs say n plus2 batch runs we're going to process from 200 to 299 assuming we are processing 100 offsets.

Just a small level of detail that the front part is inclusive and the end part is exclusive. If you're looking for more details of what exactly is stored in the checkpoint, I have a whole video on that. I'll leave a link to that in the description of this blog. There I talk about each of these uh different folders. What do they contain? Stuff like that.

---

# End of Combined Transcripts

---

*This document was automatically generated on 2026-02-06 from 23 transcript files.*

*For the latest content, visit: https://www.youtube.com/@CanadianDataGuy*





---
*Skill auto-generated from YouTube transcript*
*Created: 2026-02-07*
