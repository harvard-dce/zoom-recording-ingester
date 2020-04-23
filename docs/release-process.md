## Release Process

### Step 1: Update master

Merge all changes for release into master.  
Checkout master branch, git pull.

### Step 2: Test release in dev stack

First check that the codebuild runs with the new changes on a dev stack:

If there are new functions you must package and ensure the code is in s3:

    invoke package -u

then

	invoke stack.update
	invoke codebuild --revision=master

Make sure codebuild completes successfully.

### Step 3: Tag management

First update your tags:

    git tag -l | xargs git tag -d
    git fetch --tags    


Then tag release:

    git tag release-vX.X.X
    git push --tags

### Step 4: Release to production stack

Make sure you are working on the correct zoom ingester stack, double check environment variables. Then:

If there are new functions you must package and ensure the code is in s3:

    invoke package -u
    
then

	invoke stack.update
	invoke codebuild --revision=release-vX.X.X
