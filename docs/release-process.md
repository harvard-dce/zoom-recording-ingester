## Release Process

Merge all changes for release into master.  
Checkout master branch, git pull.

First check that the codebuild runs with the new changes on a dev stack:

	invoke stack.update
	invoke codebuild --revision=master

Make sure this passes, then:

Tag release:

    git tag release-vX.X.X
    git push --tags

Make sure you are working on the correct zoom ingester stack, double check environment variables. Then:

	invoke stack.update
	invoke codebuild --revision release-vX.X.X
