import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()


def DatasetSplitter(inputdataset, filesPerJob, maxFiles=None):
    logger.debug("DatasetSplitter")
    if maxFiles is None:
        logger.info("Using entire dataset for splitting")
    else:
        logger.info('Using first %i files of dataset for splitting' % maxFiles)
    input = inputdataset.files[:maxFiles]

    for i in range(0, len(input), filesPerJob):
        yield input[i: i + filesPerJob]
