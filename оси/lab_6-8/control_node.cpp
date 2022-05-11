#include <unistd.h>
#include <iostream>
#include <ctime>
#include "my_zmq.h"
#include "Balance_Tree.h"

using node_id_type = long long;


int main() {
    bool ok;
    Balance_Tree<node_id_type> *control_node = new Balance_Tree<node_id_type>(-1, nullptr);
    std::string s;
    node_id_type id;
    std::cout << "\t\tUsage" << std::endl;
    std::cout << "Create id: create calculation node" << std::endl;
    std::cout << "heartbit time: set time to heartbit" << std::endl;
    std::cout << "Remove id: delete calculation node with id $id and all id's children" << std::endl;
    std::cout << "Exec id key val: add [key, val] add local dictionary" << std::endl;
    std::cout << "Exec id key: check local dictionary" << std::endl;
    std::cout << "Print 0: print topology" << std::endl;
    std::pair<void *, void *> child; // context, socket
    while (std::cin >> s >> id) {
        if (s == "create") {
            auto *tree = find(id, control_node);
            if (tree != nullptr) {
                std::cout << "Node with id " << id << " already exists" << std::endl;
                continue;
            }
            if (control_node->get_data() == -1) {
                zmq::init_pair_socket(child.first, child.second);
                if (zmq_bind(child.second, ("tcp://*:" + std::to_string(PORT_BASE + id)).c_str()) != 0) {
                    perror("ZMQ_Bind");
                    exit(EXIT_FAILURE);
                }
                int fork_id = fork();
                if (fork_id == 0) {
                    execl(NODE_EXECUTABLE_NAME, NODE_EXECUTABLE_NAME, std::to_string(id).c_str(), nullptr);
                    perror("Execl");
                    exit(EXIT_FAILURE);
                } else if (fork_id > 0) {
                    control_node->set_data(id);
                } else {
                    perror("Fork");
                    exit(EXIT_FAILURE);
                }
            } else {
                auto inst = control_node->find_insert();
                auto *msg = new msg_t({fail, inst.first->get_data(), id});
                (inst.second) ? msg->action = create_left : msg->action = create_right;
                msg_t reply = *msg;
                zmq::send_receive_wait(msg, reply, child.second);
                if (reply.action == success) {
                    control_node->insert(id);
                } else {
                    std::cout << "Error: Parent is unavailable" << std::endl;
                }
            }
        } else if (s == "remove") {
            auto *tree = find(id, control_node);
            if (tree == nullptr) {
                std::cout << "Error: Node with id " << id << " doesn't exists" << std::endl;
                continue;
            }
            auto *msg = new msg_t({destroy, tree->get_parent()->get_data(), id});
            msg_t reply = *msg;
            zmq::send_receive_wait(msg, reply, child.second);
            if (reply.action == success) {
                std::cout << "Node " << id << " was deleted" << std::endl;
                remove(id, control_node);
            } else {
                std::cout << "Parent is unavailable" << std::endl;
            }
        } else if (s == "heartbit") {
            clock_t time = id;
            for (int i = 0; i < 10; ++i) {
                clock_t start = clock();
                auto *msg = new msg_t({ping, 1, control_node->get_data()});
                msg_t reply = *msg;
                zmq::send_msg_no_wait(msg, child.second);
                while (clock() - start < 4 * time) {
                    zmq::receive_msg(reply, child.second);
                    std::cout << "OK: " << reply.id << ": " << reply.parent_id << std::endl;
                }
                std::cout << std::endl;
            }
        } else if (s == "exec") {
            auto *tree = find(id, control_node);
            if (tree == nullptr) {
                std::cout << "Error: Node doesn't exists" << std::endl;
                continue;
            }
            ok = true;
            std::string key;
            char c;
            int val = -1;
            bool add = false;
            std::cin >> key;
            if ((c = getchar()) == ' ') {
                add = true;
                std::cin >> val;
            }
            key += SENTINEL;
            for (auto i: key) {
                auto *token = new msg_t({fail, i, id});
                (add) ? token->action = exec_add : token->action = exec_check;
                msg_t reply({fail, id, id});
                if (!zmq::send_receive_wait(token, reply, child.second) or reply.action != success) {
                    std::cout << "Fail: " << i << std::endl;
                    ok = false;
                    break;
                }
            }
            if (add) {
                auto *msg = new msg_t({exec_add, val, id});
                msg_t reply = *msg;
                if (!(zmq::send_receive_wait(msg, reply, child.second) and reply.action == success)) {
                    std::cout << "Fail" << std::endl;
                    ok = false;
                    break;
                }
            }
            if (!ok) {
                std::cout << "Error: Node is unavailable" << std::endl;
            }
        } else if (s == "print") {
            std::cout << control_node;
        }
    }
    std::cout << "Out tree:" << std::endl;
    std::cout << control_node;
    auto *msg = new msg_t({destroy, -1, control_node->get_data()});
    msg_t reply = *msg;
    zmq::send_receive_wait(msg, reply, child.second);
    zmq_close(child.second);
    zmq_ctx_destroy(child.first);
    return 0;
}