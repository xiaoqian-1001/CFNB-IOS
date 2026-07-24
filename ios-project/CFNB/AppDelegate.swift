import UIKit

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?
    
    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        window = UIWindow(frame: UIScreen.main.bounds)
        let vc = UIViewController()
        vc.view.backgroundColor = .black
        let label = UILabel()
        label.text = "CFNB - Cloudflare IP Optimizer"
        label.textAlignment = .center
        label.textColor = .white
        label.frame = CGRect(x: 50, y: 200, width: 300, height: 50)
        vc.view.addSubview(label)
        window?.rootViewController = vc
        window?.makeKeyAndVisible()
        return true
    }
}
