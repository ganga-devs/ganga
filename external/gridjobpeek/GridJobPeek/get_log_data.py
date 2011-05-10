
'''get_log_data

Contains the functions used to retrieve sections of a jobs log file.

Contains functionality to:
- return an arbitrary number of lines from the end of a given file
- return any new lines added to a file since last time it was "peeked" at
'''

#--------------------------------------------------------------------------#

def tail_file(follow, number_lines, log_file, action_id,
              current_tails, block_size, msg_size_limit):
    '''Function will:
     - return the last n lines of a log file
     - then return any lines that are added to it between sequential peeks'''

    #if the file has not been peeked at before get the last n lines of it  
    if not current_tails.has_key(action_id):
        try:
            file_obj = open(log_file, 'r')
            file_obj.seek(0,0)
            log_data = get_n_lines(number_lines, file_obj, block_size)

            #if continued monitoring of the file if required
            if follow == True:
                #store file object for use next time round
                current_tails[action_id] = file_obj
                
            elif follow == False:
                file_obj.close()
        except IOError:
            print 'Log file not found', log_file

    #if file has been peeked at before get any new lines added
    else:
        file_obj = current_tails[action_id]
        log_data = file_obj.read()

    if len(log_data) > msg_size_limit:
        #if log data exceeds size limit - take only the last section
        log_data=log_data[(len(log_data) - msg_size_limit):len(log_data)]

    return log_data, current_tails


def get_n_lines(number_lines, f, block_size):
    '''Function to return the last n lines of a file'''

    #if n = -1 return the entire file
    if number_lines == -1:
        f.seek(0,0)
        log_data = f.read()

    else:
        f.seek(0,2)                      
        bytes = f.tell()
        lines_to_find = int(number_lines)
        #initialise block parameters
        block = 0     

        #loop until find required number of lines
        while lines_to_find > 0:
            
            #take a section of the file
            block += 1
            section = block*block_size     

            #if possible go to the start of the section
            if bytes - section > 0:
                f.seek(-1*section,2)
                f.readline()

                #find number of complete lines in the section    
                lines = f.read()
                lines_found = lines.count('\n')
                lines_to_find = number_lines-lines_found

            #for smaller files take entire file
            else:
                f.seek(0,0)
                lines = f.read()
                lines_found = lines.count('\n')

                #count number of lines in that section and update counter 
                lines_to_find = number_lines - lines_found
                break

        start = 0
        #correct for if too many lines have been found
        if lines_to_find < 0:
            i = 0 
            while i > lines_to_find:
                index = lines.find('\n', start)
                start = index + 1
                i -= 1

        log_data = lines[start:]

    return log_data


