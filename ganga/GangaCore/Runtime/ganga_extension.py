def error_handler(exception_obj, etype, value, tb, tb_offset=None):
    """
    Error handler for IPython 3.x+ to identify expected Ganga exceptions or unexpected uncaught exceptions from somewhere
    """
    ## see https://ipython.org/ipython-doc/dev/api/generated/IPython.core.interactiveshell.html#IPython.core.interactiveshell.InteractiveShell.set_custom_exc
    from GangaCore.Utility.logging import getLogger
    logger = getLogger()
    #logger.error("%s" % value)

    from GangaCore.Core.exceptions import GangaException
    import traceback
    # Extract the stack from this traceback object
    stack = traceback.extract_tb(tb)
    # If this is an error from the interactive prompt then the length is 2, otherwise the error is from deeper in Ganga
    if not issubclass(etype, GangaException) and len(stack) > 2:
        logger.error("!!Unknown/Unexpected ERROR!!")
        logger.error("If you're able to reproduce this please report this to the Ganga developers!")
        #logger.error("value: %s" % value)
        exception_obj.showtraceback((etype, value, tb), tb_offset=tb_offset)
    else:
        logger.error("%s" % value)
    return None

def ganga_prompt(_=None):
    #Flush the logging
    from GangaCore.Utility.logging import flushAtIPythonPrompt
    flushAtIPythonPrompt()

    try:
        from GangaCore.GPIDev.Credentials import get_needed_credentials

        needed_credentials = get_needed_credentials()

        # Add still-needed credentials to the prompt
        if needed_credentials:
            prompt = 'Warning, some credentials needed by the monitoring are missing or invalid:\n'
            for cred_req in needed_credentials:
                prompt += '  ' + str(cred_req).replace('\n ', '') + '\n'
            prompt += 'Call `credential_store.renew()` to update them.\n'
            print(prompt)
    except KeyboardInterrupt:
        return

def load_ipython_extension(ipython):
    # The `ipython` argument is the currently active `InteractiveShell`
    # instance, which can be used in any way. This allows you to register
    # new magics or aliases, for example.
    from GangaCore.Runtime.IPythonMagic import GangaMagics
    ipython.register_magics(GangaMagics)
    ipython.events.register("post_execute", ganga_prompt)
    ipython.set_custom_exc((Exception,), error_handler)
