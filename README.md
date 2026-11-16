# Les bibliothèques d'IshedDB

[IshedDB](https://isheddb.com) est une base de données de documents hébergée, compatible avec l'API de MongoDB. Chaque
bibliothèque reprend **les noms et les formes du pilote MongoDB officiel de son langage** : le code écrit pour MongoDB
change à peine.

| Langage | Dossier | Comme | Installer (une fois publiée) |
|---|---|---|---|
| Node.js | `node/` | le pilote `mongodb` | `npm install @ished/client` |
| Python | `python/` | PyMongo | `pip install ished` |
| Ruby | `ruby/` | le gem `mongo` | `gem install ished` |
| PHP | `php/` | `mongodb/mongodb` | `composer require isheddb/ished` |
| Go | `go/` | `mongo-go-driver` | `go get github.com/treecode9-arch/ished-drivers/go` |
| Java | `java/` | le pilote Java | Maven `com.isheddb:ished` |
| C# (.NET) | `dotnet/` | `MongoDB.Driver` | `dotnet add package Ished` |
| C++ | `cpp/` | `mongocxx` | un seul en-tête, `include/ished/ished.hpp` (libcurl, nlohmann/json) |

```python
from ished import connect
clients = connect("https://isheddb.com", key).db("boutique")["clients"]
for c in clients.find({"ville": "Dakar"}).sort("âge", -1):
    print(c["nom"])
```

La clé d'un projet (`ished_sk_…`) se crée dans la console d'IshedDB ; elle ne voyage qu'en HTTPS et ne doit jamais
être dans un navigateur. Les fichiers (comme GridFS) : `GridFSBucket` dans chaque langage.

Documentation : https://isheddb.com/docs · Licence : Apache 2.0 (`LICENSE`).

Ce dépôt est construit à partir du dépôt d'IshedDB ; une proposition de changement y est reportée par l'équipe.
