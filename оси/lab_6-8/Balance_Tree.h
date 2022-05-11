#ifndef BALANCE_TREE_H
#define BALANCE_TREE_H

#include <iostream>

template<typename T>
class Balance_Tree {
public:
    Balance_Tree(T id, Balance_Tree<T> *parent) : data(id), left(nullptr), right(nullptr), parent(parent) {};

    void insert(T id) {
        if (nodes_left <= nodes_right) {
            if (left != nullptr) {
                left->insert(id);
            } else {
                left = new Balance_Tree<T>(id, this);
            }
            ++nodes_left;
            return;
        }
        if (right != nullptr) {
            right->insert(id);
        } else {
            right = new Balance_Tree<T>(id, this);
        }
        ++nodes_right;
    }

    std::pair<Balance_Tree<T> *, bool>
    find_insert() { // возвращает узел, куда произойдет вставка и true - если вставится в левый узел, false - в правый.
        if (nodes_left <= nodes_right) {
            if (left != nullptr) {
                return left->find_insert();
            } else {
                return std::make_pair(this, true);
            }
        }
        if (right != nullptr) {
            return right->find_insert();
        }
        return std::make_pair(this, false);
    }

    bool empty() {
        return (this->nodes_left + this->nodes_right) == 0;
    }

    template<typename S>
    friend Balance_Tree<S> *find(S search, Balance_Tree<S> *t);

    template<typename S>
    friend std::ostream &operator<<(std::ostream &os, Balance_Tree<S> *t);

    template<typename S>
    friend void print_tree(std::ostream &os, Balance_Tree<S> *t, size_t x);

    T get_data() {
        return data;
    }

    void set_data(T id) {
        data = id;
    }

    Balance_Tree<T> *get_parent(){
        return this->parent;
    }

    template<typename S>
    friend void remove(S id, Balance_Tree<S> *t);

private:
    T data;
    Balance_Tree<T> *left, *right, *parent;
    size_t nodes_left = 0, nodes_right = 0;
};

template<typename T>
void print_tree(std::ostream &os, Balance_Tree<T> *t, size_t x) {
    if (t == nullptr) {
        return;
    }
    print_tree(os, t->right, x + 1);
    for (size_t i = 0; i < x; ++i) {
        os << '\t';
    }
    os << t->data << std::endl;
    print_tree(os, t->left, x + 1);
}

template<typename T>
std::ostream &operator<<(std::ostream &os, Balance_Tree<T> *t) {
    print_tree(os, t, 0);
    return os;
}

template<typename T>
Balance_Tree<T> *find(T id, Balance_Tree<T> *t) { // поиск узла дерева. Производится обход в порядке КЛП
    if (t->data == id) {
        return t;
    }
    Balance_Tree<T> *tree = nullptr;
    if (t->left != nullptr) {
        tree = find(id, t->left);
    }
    if (t->right != nullptr and tree == nullptr) {
        tree = find(id, t->right);
    }
    return tree;
}


template<typename T>
void remove(T id,
            Balance_Tree<T> *t) { // удаляется узел и его потомки. Затем уменьшаем количество левых (правых) узлов у родителей
    if (t == nullptr) {
        return;
    }
    if (t->left != nullptr and t->left->data == id) {
        int n = t->nodes_left;
        delete t->left;
        t->left = nullptr;
        auto *tree = t;
        t->nodes_left = 0;
        while (tree->parent != nullptr) {
            if (tree->parent->right ==
                tree) { // определяем, является текущий узел левым и правым потомком, затем уменьшаем количество левых (правых) узлов у родителя.
                tree->parent->nodes_right -= n;
            } else {
                tree->parent->nodes_left -= n;
            }
            tree = tree->parent;
        }
        return;
    }
    if (t->right != nullptr and t->right->data == id) {
        int n = t->nodes_right;
        delete t->right;
        t->right = nullptr;
        t->nodes_right = 0;
        auto *tree = t;
        while (tree->parent != nullptr) {
            if (tree->parent->right ==
                tree) { // определяем, является текущий узел левым и правым потомком, затем уменьшаем количество левых (правых) узлов у родителя.
                tree->parent->nodes_right -= n;
            } else {
                tree->parent->nodes_left -= n;
            }
            tree = tree->parent;
        }
        return;
    }
    if (t->left != nullptr) {
        remove(id, t->left);
    }
    if (t->right != nullptr) {
        remove(id, t->right);
    }
}

#endif //BALANCE_TREE_H
