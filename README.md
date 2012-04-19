# TastyMongo
Light-weight REST API implementation for MongoEngine (MongoDB) on Pyramid, 
based on django-tastypie.

# TODO

## better seperation of concerns

The Tastypie codebase has grown rather unwieldy.  It has various dependencies
on Django that can be separated and Tastypie's own sparse files have
unnecessary interdependencies that can be refactored. 

Low hanging fruit:

 * Move ``dispatch`` and friends to api.py since it has little to do with 
   specific resources
 * Simplify the call signature of the many small functions by creating a bundle
   earlier in the flow and using the bundle further down the flow
 * Differentiate between ``bundle.data_in`` and ``bundle.data_out``
 * Separate out a Filter class to remove django-specific
 * Remove ``to_simple`` from Serializer class since it introduces the Serializer
   to know about 'Bundles' and about internal field properties, like ``is_m2m``
 * Merge and refactor several separate but very similar ``dehydrate`` functions
 * Improve naming of variables, notably ``is_m2m`` and ``bundle.obj`` can have
   ambiguous content.
 * Add a lot more clarifying comments 
