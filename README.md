# Goda - Go Data Administrator

The Go Data Administrator (Goda) is a package designed to take care of the actual data storage of structs.
Similar in usage to the ````encoding/json```` package, it uses struct tags and reflection to store your data.


## Features
  1. Storage and Retrieval of Go structs.

Well, that is pretty much it, but what more could you want?
More features is added as I need them.

## Limitations
	1. Only flat structs is supported. This could change in future where struct types in structs are interpreted differently by different storers and retrievers.
	2. Currently, there is only a database backend.
	3. Database backend only supports postgreSQL (and others with equivalent syntax.)
	4. Database backend does not create databases or tables (That would break interfaces bacause of difficulty in determining initialization need).
	
## Roadmap
	Soon:
		1. Retreivers
		2. Formalizing usage
	
	Later:
		1. More backends
		2. Map as storable type.
	
	Maybe:
		1. Nestling
		