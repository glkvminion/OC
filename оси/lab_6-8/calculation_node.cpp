#include "my_zmq.h"
#include <iostream>
#include <map>
#include <unistd.h>

long long node_id;

int main(int argc, char **argv) {
    if (argc != 2) {
        exit(EXIT_FAILURE);
    }
    std::string key;
    int val;
    std::map<std::string, int> dict;
    node_id = std::stoll(std::string(argv[1]));

    void *node_parent_context = zmq_ctx_new();
    void *node_parent_socket = zmq_socket(node_parent_context, ZMQ_PAIR);
    if (zmq_connect(node_parent_socket, ("tcp://localhost:" + std::to_string(PORT_BASE + node_id)).c_str()) != 0) {
        perror("ZMQ_Connect");
        exit(EXIT_FAILURE);
    }
    long long left_id = -1, right_id = -1;
    std::pair<void *, void *> left, right; // <context, socket>
    std::cout << "OK: " << getpid() << std::endl;
    bool has_left = false, has_right = false, awake = true, add = false;
    while (awake) {
        msg_t token({fail, 0, 0});
        zmq::receive_msg(token, node_parent_socket);
        auto *reply = new msg_t({fail, node_id, node_id});
        if (token.action == create_left) {
            if (token.parent_id == node_id) {
                zmq::init_pair_socket(left.first, left.second);
                if (zmq_bind(left.second, ("tcp://*:" + std::to_string(PORT_BASE + token.id)).c_str()) != 0) {
                    perror("Bind");
                    exit(EXIT_FAILURE);
                }
                int fork_id = fork();
                if (fork_id == 0) {
                    execl(NODE_EXECUTABLE_NAME, NODE_EXECUTABLE_NAME, std::to_string(token.id).c_str(), nullptr);
                    perror("Execl");
                    exit(EXIT_FAILURE);
                } else if (fork_id > 0) {
                    has_left = true;
                    left_id = token.id;
                    reply->action = success;
                } else {
                    perror("Fork");
                    exit(EXIT_FAILURE);
                }
            } else {
                if (has_left) {
                    auto *token_left = new msg_t(token);
                    msg_t reply_left = *reply;
                    zmq::send_receive_wait(token_left, reply_left, left.second);
                    if (reply_left.action == success) {
                        *reply = reply_left;
                    }
                }
                if (has_right and reply->action != success) {
                    auto *token_right = new msg_t(token);
                    msg_t reply_right = *reply;
                    zmq::send_receive_wait(token_right, reply_right, right.second);
                    *reply = reply_right;
                }
            }
        } else if (token.action == create_right) {
            if (token.parent_id == node_id) {
                zmq::init_pair_socket(right.first, right.second);
                if (zmq_bind(right.second, ("tcp://*:" + std::to_string(PORT_BASE + token.id)).c_str()) != 0) {
                    perror("Bind");
                    exit(EXIT_FAILURE);
                }
                int fork_id = fork();
                if (fork_id == 0) {
                    execl(NODE_EXECUTABLE_NAME, NODE_EXECUTABLE_NAME, std::to_string(token.id).c_str(), nullptr);
                    perror("Execl");
                    exit(EXIT_FAILURE);
                } else if (fork_id > 0) {
                    has_right = true;
                    right_id = token.id;
                    reply->action = success;
                } else {
                    perror("Fork");
                    exit(EXIT_FAILURE);
                }
            } else {
                if (has_left) {
                    auto *token_left = new msg_t(token);
                    msg_t reply_left = *reply;
                    zmq::send_receive_wait(token_left, reply_left, left.second);
                    if (reply_left.action == success) {
                        *reply = reply_left;
                    }
                }
                if (has_right and reply->action != success) {
                    auto *token_right = new msg_t(token);
                    msg_t reply_right = *reply;
                    zmq::send_receive_wait(token_right, reply_right, right.second);
                    *reply = reply_right;
                }
            }
        } else if (token.action == ping) {
            msg_t ping_left = *reply, ping_right = *reply;
            ping_left.action = ping_right.action = fail;
            reply->action = success;
            reply->parent_id = 1;
            if (has_left) {
                auto *msg_left = new msg_t({ping, -1, left_id});
                zmq::send_receive_wait(msg_left, ping_left, left.second);
                if (ping_left.action != success) {
                    ping_left.action = success;
                    ping_left.parent_id = 0;
                }
            }
            if (has_right) {
                auto *msg_right = new msg_t({ping, -1, right_id});
                zmq::send_receive_wait(msg_right, ping_right, right.second);
                if (ping_right.action != success) {
                    ping_right.action = success;
                    ping_right.parent_id = 0;
                }
            }
            if (ping_right.action == success) {
                zmq::send_msg_no_wait(&ping_right, node_parent_socket);
            }
            if (ping_left.action == success) {
                zmq::send_msg_no_wait(&ping_left, node_parent_socket);
            }
        } else if (token.action == destroy) {
            if (token.parent_id == node_id) {
                auto *destroy_child = new msg_t({destroy, -1, token.id});
                msg_t reply_child = *destroy_child;
                if (has_left and left_id == token.id) {
                    zmq::send_receive_wait(destroy_child, reply_child, left.second);
                    has_left = false;
                    left_id = -1;
                    zmq_close(left.second);
                    zmq_ctx_destroy(left.first);
                    left.first = left.second = nullptr;
                } else {
                    zmq::send_receive_wait(destroy_child, reply_child, right.second);
                    has_right = false;
                    right_id = -1;
                    zmq_close(right.second);
                    zmq_ctx_destroy(right.first);
                }
                *reply = reply_child;
            } else if (token.id == node_id) {
                if (has_left) {
                    auto *destroy_left = new msg_t({destroy, left_id, left_id});
                    msg_t reply_left = *destroy_left;
                    zmq::send_receive_wait(destroy_left, reply_left, left.second);
                    has_left = false;
                    left_id = -1;
                    zmq_close(left.second);
                    zmq_ctx_destroy(left.first);
                    left.first = left.second = nullptr;
                }
                if (has_right) {
                    auto *destroy_right = new msg_t({destroy, left_id, left_id});
                    msg_t reply_right = *destroy_right;
                    zmq::send_receive_wait(destroy_right, reply_right, right.second);
                    has_right = false;
                    right_id = -1;
                    zmq_close(right.second);
                    zmq_ctx_destroy(right.first);
                    right.first = right.second = nullptr;
                }
                awake = false;
                reply->action = success;
            } else {
                auto *token_left = new msg_t(token);
                msg_t reply_left = *reply;
                zmq::send_receive_wait(token_left, reply_left, left.second);
                if (reply_left.action == success) {
                    *reply = reply_left;
                }
            }
            if (has_right and reply->action != success) {
                auto *token_right = new msg_t(token);
                msg_t reply_right = *reply;
                zmq::send_receive_wait(token_right, reply_right, right.second);
                *reply = reply_right;
            }
        } else if (token.action == exec_check) {
            if (token.id == node_id) {
                char c = token.parent_id;
                if (c == SENTINEL) {
                    if (dict.find(key) != dict.end()) {
                        std::cout << "OK:" << node_id << ":" << dict[key] << std::endl;
                    } else {
                        std::cout << "OK:" << node_id << ":'" << key << "' not found" << std::endl;
                    }
                    reply->action = success;
                    key = "";
                } else {
                    key += c;
                    reply->action = success;
                }
            } else {
                auto *token_left = new msg_t(token);
                msg_t reply_left = *reply;
                zmq::send_receive_wait(token_left, reply_left, left.second);
                if (reply_left.action == success) {
                    *reply = reply_left;
                }
            }
            if (has_right and reply->action != success) {
                auto *token_right = new msg_t(token);
                msg_t reply_right = *reply;
                zmq::send_receive_wait(token_right, reply_right, right.second);
                *reply = reply_right;
            }
        } else if (token.action == exec_add) {
            if (token.id == node_id) {
                char c = token.parent_id;
                if (c == SENTINEL) {
                    add = true;
                    reply->action = success;
                } else if (add) {
                    val = token.parent_id;
                    dict[key] = val;
                    std::cout << "OK:" << node_id << std::endl;
                    add = false;
                    key = "";
                    reply->action = success;
                } else {
                    key += c;
                    reply->action = success;
                }
            } else {
                auto *token_left = new msg_t(token);
                msg_t reply_left = *reply;
                zmq::send_receive_wait(token_left, reply_left, left.second);
                if (reply_left.action == success) {
                    *reply = reply_left;
                }
            }
            if (has_right and reply->action != success) {
                auto *token_right = new msg_t(token);
                msg_t reply_right = *reply;
                zmq::send_receive_wait(token_right, reply_right, right.second);
                *reply = reply_right;
            }
        }
        zmq::send_msg_no_wait(reply, node_parent_socket);
    }
    zmq_close(node_parent_socket);
    zmq_ctx_destroy(node_parent_context);
}