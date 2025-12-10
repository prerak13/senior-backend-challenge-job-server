-- Users
INSERT INTO users (id, username, email) VALUES (1, 'U0', 'u0@example.com');
INSERT INTO users (id, username, email) VALUES (2, 'U1', 'u1@example.com');
INSERT INTO users (id, username, email) VALUES (3, 'U2', 'u2@example.com');
INSERT INTO users (id, username, email) VALUES (4, 'U3', 'u3@example.com');
INSERT INTO users (id, username, email) VALUES (5, 'U4', 'u4@example.com');

-- Projects (assigned round robin to users)
INSERT INTO projects (id, name, description, user_id) VALUES (1, 'P0', 'Project P0', 1);
INSERT INTO projects (id, name, description, user_id) VALUES (2, 'P1', 'Project P1', 2);
INSERT INTO projects (id, name, description, user_id) VALUES (3, 'P2', 'Project P2', 3);
INSERT INTO projects (id, name, description, user_id) VALUES (4, 'P3', 'Project P3', 4);
INSERT INTO projects (id, name, description, user_id) VALUES (5, 'P4', 'Project P4', 5);
INSERT INTO projects (id, name, description, user_id) VALUES (6, 'P5', 'Project P5', 1);
INSERT INTO projects (id, name, description, user_id) VALUES (7, 'P6', 'Project P6', 2);
INSERT INTO projects (id, name, description, user_id) VALUES (8, 'P7', 'Project P7', 3);
INSERT INTO projects (id, name, description, user_id) VALUES (9, 'P8', 'Project P8', 4);
INSERT INTO projects (id, name, description, user_id) VALUES (10, 'P9', 'Project P9', 5);
