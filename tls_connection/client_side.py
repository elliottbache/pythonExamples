import ssl
import socket
import random
import string
import hashlib
import os
import sys
import time
import base64
import concurrent.futures
import multiprocessing
import subprocess

cpp_binary_path = "./pow_benchmark" # path to c++ executable
threads = "2" # number of threads used in c++ code to find hash
valid_messages = {
    "HELO", "POW", "ERROR", "END", "NAME", "MAILNUM", "MAIL1", "MAIL2",
    "SKYPE", "BIRTHDATE", "COUNTRY", "ADDRNUM", "ADDRLINE1", "ADDRLINE2"
} # valid first arguments coming from the server
responses = {
    "NAME": "Elliott Bache",
    "MAILNUM": "2",
    "MAIL1": "elliottbache@gmail.com",
    "MAIL2": "elliottbache2@gmail.com",
    "SKYPE": "elliottbache@hotmail.com",
    "BIRTHDATE": "99.99.1982",
    "COUNTRY": "USA",
    "ADDRNUM": "2",
    "ADDRLINE1": "234 Evergreen Terrace",
    "ADDRLINE2": "Springfield"
}

authdata = "" # authorization data received from server
hostname = 'localhost' # This PC
ports = [3336, 8083, 8446, 49155, 3481, 65532]
private_key_path = 'ec_private_key.pem' # File path for the EC private key
client_cert_path = 'client_cert.pem' # File path for the client certificate
pow_timeout = 7200 # timeout for pow in seconds
all_timeout = 6 # timeout for all function except pow in seconds

def tls_connect():
    """
    Creates a connection to the remote server defined in the global variables
    Inputs: None
    Outputs: socket
    """
    # Create the client socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Create an SSL context
    context = ssl.create_default_context()

    # Load the client's private key and certificate
    context.load_cert_chain(certfile=client_cert_path, keyfile=private_key_path)

    # Disable server certificate verification (not recommended for production)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    return context.wrap_socket(client_socket, server_hostname=hostname)


def hasher(authdata,input_string):
    """
    Hashes as string using SHA1.
    Inputs: authdata = auth data from the server, input_string = an ASCII string
    Outputs: cksum_in_hex = checksum in hex
    """
    to_be_hashed = authdata + input_string
    cksum_in_hex = hashlib.sha1(to_be_hashed.encode()).hexdigest()

    return cksum_in_hex

def decipher_message(message):
    """
    Read message and do error checking
    Inputs: message = in UTF-8
    Outputs: err = 0 if no error or 1 if decoding error or 2 if invalid command,
            args = message split into list
    """
    # check that we have a UTF-8 message
    try:
        smessage = message.decode('utf-8').replace("\n", "")
    except Exception as e:
        print ("string is not valid: ",e)
        print ("string is probably not UTF-8")
        return 1, ""

    args = smessage.split()

    if not args:
        print("No args in the response")
        return 2, ""

    # check that message belongs to list of possible messages
    if args[0] not in valid_messages:
        print("This response is not valid: ",smessage)
        return 2, ""

    # if only 1 argument is received add another empty string argument
    # to avoid errors since server is supposed to send 2 args.  
    if len(args) == 1: 
        args.append("")

    return 0, args

def has_leading_zero_bits(digest, full_bytes, remaining_bits):
    """
    Checks whether the digest starts with 0's in the first full_bytes and 
    remaining_bits
    Inputs: digest = digest from the SHA1, full_bytes = number of full bytes
        given by the difficulty, remaining_bits = the bits that along with 
        full_bytes is equal to the difficulty
    """
    # Check full zero bytes
    for i in range(full_bytes):
        if digest[i] != 0:
            return False

    # Check partial byte if needed
    if remaining_bits:
        mask = 0xFF << (8 - remaining_bits) & 0xFF
        if digest[full_bytes] & mask:
            return False

    return True

def handle_pow_cpp(authdata,difficulty):
    """
    Takes the authdata and difficulty and finds a suffix that will reproduce
    a hash with the given number of leading zeros
    Inputs: authdata = input from the server, difficulty = number 
        of leading in the valid hash
    Outputs: error code, formatted response in UTF-8
    """
    # error check authdata
    if not isinstance(authdata, str):
        print("authdata is not a string.  Exiting since hashing function " 
                "will not work correctly")
        return 4, "\n".encode()

    # error check difficulty
    try:
        idifficulty = int(float(difficulty))
        print(f"POW difficulty is {idifficulty}")
    except:
        print("POW difficulty is not an integer")
        return 4, "\n".encode()


    # run pre-compiled c++ code for finding suffix
    try:
        result = subprocess.run(
            [cpp_binary_path, authdata, difficulty, threads],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )

        # Extract the single result line
        suffix = None
        for line in result.stdout.splitlines():
            if line.startswith("RESULT:"):
                suffix = line[len("RESULT:"):]
                break

        if suffix:
            print(f"✅ Valid POW Suffix: {suffix} {hashlib.sha1((authdata + suffix).encode()).hexdigest()}")
            return 0, (suffix + "\n").encode()
        else:
            print("❌ No RESULT found in output.")
            return 4, "\n".encode()

    except subprocess.CalledProcessError as e:
        print("❌ Error running executable:")
        print(e.stderr)

    return 4, "\n".encode()

def define_response(args):
    """
    Create response to message depending on recevied message
    Inputs: args = a list of the arguments from the server's response
    Outputs: err = 0 if ok or 1 if END was received or 2 if ERROR or was 
        received or 3 if timeout or 4 for other invalid messages, 
        response = to be sent to server in UTF-8
    """
    global authdata
   
    if args[0] == "HELO":
        return 0, "EHLO\n".encode()
    if args[0] == "END":
        return 1, "OK\n".encode()
    if args[0] == "ERROR":
        print ("Server error: " + " ".join(args[1:]))
        return 2, "\n".encode()
    if args[0] == "POW":
        authdata, difficulty = args[1], args[2]

        for i in range(1):
            # record start time
            start = time.time()
            return_list = handle_pow_cpp(authdata, difficulty)
            # record end time
            end = time.time()
            # print the difference between start
            # and end time in milli. secs
            print("The time of execution of above program is :",
              (end-start) , "s")

        return return_list[0], return_list[1]

    if args[0] in valid_messages:
        print("authdata = ",authdata)
        return 0, (hasher(authdata,args[1]) + " " + responses[args[0]] + "\n").encode()

    return 4, "\n".encode()

def define_response_worker(args, queue):
    """
    Calls response function, queuing up the results
    Inputs: args = args to be sent for response definition, queue = 
        previously defined job queue
    Outputs: None
    """

    err, result = define_response_with_newline(args)
    results = [err, result, authdata]
    queue.put(results)

def define_response_with_newline(args):
    """
    Calls response function, ensuring results have a newline at the end
    Inputs: args = args to be sent for response definition
    Outputs: err = error code, response = response to be sent to server
    """

    # Create response to message from message
    err, response = define_response(args)

    # double check that newline has been placed on string
    if not response.decode('utf-8').endswith("\n"):
        print ("string does not end with new line")
        to_encode = response.decode('utf-8') + "\n"
        response = to_encode.encode()

    return err, response

def main():

    # Create and wrap socket
    secure_sock = tls_connect()

    # Connect to the server using TLS
    # Cycle through possible ports, trying to connect to each until success
    is_connected = False
    for port in ports:
        if not is_connected:
            try:
                secure_sock.connect((hostname,int(port)))
                is_connected = True
                print(f"Connected to {port}\n")
            except:
                print(f"Cannot connect to {port}")

    if not is_connected:
        print("Not able to connect to any port.  Exiting")
        sys.exit()

    while True:

        # Receive the message from the server
        message = ''
        message = secure_sock.recv(1024)
        print(f"received = {message}")

        # If nothing is received wait 6 seconds and continue
        if message == b"":
            print("received empty message.  continuing.")
            sys.exit()
            continue

        # Error check message and create list from message
        err, args = decipher_message(message)

        # If no args are received, continue
        if err:
            print(f"Problem deciphering message. Error code = {err}.  continuing.")
            continue

        # Define timeouts
        if args and args[0] == "POW":
            this_timeout = pow_timeout
        else:
            this_timeout = all_timeout

        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=define_response_worker, args=(args, queue))
        p.start()
        p.join(timeout=this_timeout)  # Wait up to 6 or 7200 seconds
        if p.is_alive():
            p.terminate()  # 🔪 Forcefully stop the process
            p.join()
            err, response = 3, "".encode()
            print(f"⏰ {args[0]} Function timed out.")
            print("Response:", response)
            print("err :", err)
            continue
        else:
            err, response, maybe_authdata = queue.get()
            global authdata
            if maybe_authdata:
                authdata = maybe_authdata

        if err == 0 or err == 1:
            # Send the response
            print(f"sending to server = {response}\n")
            secure_sock.send(response)

        # If END received from server, break and close connection
        if err == 1 or err == 2:
            break

    # Close the connection
    print("close connection")
    secure_sock.close()

if __name__ == "__main__":
    main()
