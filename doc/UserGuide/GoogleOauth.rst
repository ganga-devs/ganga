Setting upi your own Google client ID
=====================================
When you use `GoogleFile` in Ganga in its default configuration you are using Ganga's client_id. This is shared between all Ganga users. There is a global rate limit on the number of queries per second that each client_id can do set by Google. It is strongly recommended to use your own client ID as the default. The default Google quota is 10 transactions per second so it is recommended to stay under that number as if you use more than that, it will cause Ganga to rate limit and make things slower.

Here is how to create your own Google Drive client ID:

Log into the Google API Console with your Google account. It doesn't matter what Google account you use. (It need not be the same account as the Google Drive you want to access)

Select a project or create a new project.

Under "ENABLE APIS AND SERVICES" search for "Drive", and enable the "Google Drive API".

Click "Credentials" in the left-side panel (not "Create credentials", which opens the wizard), then "Create credentials"

If you already configured an "Oauth Consent Screen", then skip to the next step; if not, click on "CONFIGURE CONSENT SCREEN" button (near the top right corner of the right panel), then select "External" and click on "CREATE"; on the next screen, enter an "Application name" ("rclone" is OK) then click on "Save" (all other data is optional). Click again on "Credentials" on the left panel to go back to the "Credentials" screen.

(PS: if you are a GSuite user, you could also select "Internal" instead of "External" above, but this has not been tested/documented so far).

Click on the "+ CREATE CREDENTIALS" button at the top of the screen, then select "OAuth client ID".

Choose an application type of "Desktop app" if you using a Google account or "Other" if you using a GSuite account and click "Create". (the default name is fine)

It will show you a client ID and client secret. These values, you should store in your `~/.gangarc` file

.. code-block:: python

    [Google]
    client_id = Put_the_value_from_the_browser_here
    client_secret = Put_the_value_from_the_browser_here

Be aware that, due to the "enhanced security" recently introduced by Google, you are theoretically expected to "submit your app for verification" and then wait a few weeks(!) for their response; in practice, you can go right ahead and use the client ID and client secret with Ganga.

You will after setting this up still need to authenticate this client ID to write into your Google account. This happens inside Ganga the first time you use `Googlefile` for uploading a file.

(Thanks to @balazer on github for these instructions.)
