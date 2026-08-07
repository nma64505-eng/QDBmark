#!/usr/bin/env python3

import sys
from pathlib import Path


ASYNC_DEPENDENCY_BLOCK = """    <dependency>
      <groupId>com.allanbank</groupId>
      <artifactId>mongodb-async-driver</artifactId>
      <version>${mongodb.async.version}</version>
    </dependency>
"""

ASYNC_REPOSITORY_BLOCK = """  <repositories>
    <repository>
      <releases>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
        <checksumPolicy>warn</checksumPolicy>
      </releases>
      <snapshots>
        <enabled>false</enabled>
        <updatePolicy>never</updatePolicy>
        <checksumPolicy>fail</checksumPolicy>
      </snapshots>
      <id>allanbank</id>
      <name>Allanbank Releases</name>
      <!-- Does not support HTTPS -->
      <url>http://www.allanbank.com/repo/</url>
      <layout>default</layout>
    </repository>
  </repositories>
"""

ASYNC_SOURCE_FILES = (
    "mongodb/src/main/java/site/ycsb/db/AsyncMongoDbClient.java",
    "mongodb/src/test/java/site/ycsb/db/AsyncMongoDbClientTest.java",
)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: patch_ycsb_mongodb.py <ycsb-source-dir>", file=sys.stderr)
        return 1

    source_dir = Path(sys.argv[1]).resolve()
    pom_path = source_dir / "mongodb/pom.xml"
    if not pom_path.exists():
        print(f"missing pom: {pom_path}", file=sys.stderr)
        return 1

    text = pom_path.read_text()
    text = text.replace(ASYNC_DEPENDENCY_BLOCK, "")
    text = text.replace(ASYNC_REPOSITORY_BLOCK, "")
    pom_path.write_text(text)

    for relative_path in ASYNC_SOURCE_FILES:
        candidate = source_dir / relative_path
        if candidate.exists():
            candidate.unlink()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
