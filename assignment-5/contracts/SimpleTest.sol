// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

contract SimpleTest {
    string public message = "Hello AMM!";
    
    function setMessage(string memory _message) public {
        message = _message;
    }
}
