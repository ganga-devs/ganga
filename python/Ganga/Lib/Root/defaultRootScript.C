void test() {
    cout << "Hello World from ROOT" << endl;
    cout << "Load Path : " << gSystem->GetDynamicPath() << endl;
    gSystem->Load("libTree");
    gSystem->Exit(0);
}
