# Deploy apps with version control in SiS

This is one way to enable version control and version-based deployment of SiS apps.

The workflow is the following, using https://taskfile.dev/ to generate a set of standard
commands for deploying, dropping and updating your app.

You'll need to copy and customize the Taskfile.yml in this folder to make these examples work for you. Run `task --list` to get the full list of commands you can run with this Taskfile.

This requires you to do some initial setup with
https://github.com/Snowflake-Labs/snowcli, which is beyond the scope of this README.

## Deploying to production

1. Only deploy the production version of the app when you have the main branch checked out
2. Run `task create` the first time you create your app in SiS

   a. With this Taskfile, unless you customize it, the app will be called `MY_APP`

3. Run `task update` when you want to update it
4. Run `task drop` if you want to delete it

## Deploying preview branches

1. When you're working on a potential update to the app, create a new branch with git
2. Make your changes to the app code and commit them
3. Run `task create_preview` to deploy a preview version of your app to SiS

   a. With this Taskfile, if your username on your computer is `flastname`, and
   your branch is called `my_new_feature`, then the preview version of the app will be called
   `PREVIEW_MY_APP_MY_NEW_FEATURE_FLASTNAME`

4. Run `task update_preview` if you make updates to the code
5. Create a pull request and add the link to the newly deployed app
6. Once your PR is merged in, run `task drop_preview` locally to
   remove the preview version of the app
7. Check out the `main` branch locally, pull to get the latest version,
   and run `task update` to update the main version of your app.
