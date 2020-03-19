## Release Process

Merge all changes for release into master.  
Checkout master branch, git pull.

### In a dev instance

First check that the codebuild runs with the new changes on a dev stack:

If there are new functions you must package and ensure the code is in s3:

    invoke package -u

then

	invoke stack.update
	invoke codebuild --revision=master

Make sure this passes, then:

Tag release:

    git tag release-vX.X.X
    git push --tags

### In prod

Make sure you are working on the correct zoom ingester stack, double check environment variables. Then:

If there are new functions you must package and ensure the code is in s3:

    invoke package -u
    
then

	invoke stack.update
	invoke codebuild --revision release-vX.X.X
